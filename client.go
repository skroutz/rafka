// Copyright 2017-2019 Skroutz S.A.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	redisproto "github.com/secmask/go-redisproto"
)

type Client struct {
	id          string
	conn        net.Conn
	cfg         Config
	ctx         context.Context
	log         *log.Logger
	monitorChan chan string

	consGID   string
	consReady bool

	consWg *sync.WaitGroup

	consumer *Consumer
	producer *Producer
}

// NewClient returns a new client. After it's no longer needed, the client
// should be closed with Close().
func NewClient(ctx context.Context, conn net.Conn, cfg Config) *Client {
	id := conn.RemoteAddr().String()

	client := &Client{
		id:          id,
		monitorChan: make(chan string, 1000),
		conn:        conn,
		cfg:         cfg,
		ctx:         ctx,
		log:         log.New(os.Stderr, fmt.Sprintf("[client-%s] ", id), log.Ldate|log.Ltime),
	}

	go client.monitorWriter()

	return client
}

// monitorWriter streams any monitor strings written to the client's monitor channel.
func (c *Client) monitorWriter() {
	writer := redisproto.NewWriter(bufio.NewWriter(c.conn))
	for monitorOutput := range c.monitorChan {
		writer.WriteSimpleString(monitorOutput)
		writer.Flush()
	}
}

// SetID sets the id for c. It returns an error if id is not in the form of
// <consumer-group>:<consumer-id>.
func (c *Client) SetID(id string) error {
	if c.consReady {
		return errors.New("Client ID is already set to " + c.id)
	}

	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return errors.New("Cannot parse group.id")
	}

	c.id = id
	c.consGID = parts[0]
	c.log.SetPrefix(fmt.Sprintf("[client-%s] ", c.id))
	c.consReady = true
	return nil
}

func (c *Client) String() string {
	return c.id
}

// registerConsumer registers a new Consumer denoted by cid.
func (c *Client) registerConsumer(
	cid ConsumerID, gid string, topics []string, cfg rdkafka.ConfigMap) (*Consumer, error) {

	if gid == "" {
		return nil, errors.New("Failed to register Consumer (attr: `group.id` is missing)")
	}

	// apparently, reusing the same config between consumers silently makes them non-operational
	kafkaCfg := rdkafka.ConfigMap{}
	for k, v := range c.cfg.Librdkafka.Consumer {
		err := kafkaCfg.SetKey(k, v)
		if err != nil {
			return nil, err
		}
	}
	for k, v := range cfg {
		err := kafkaCfg.SetKey(k, v)
		if err != nil {
			return nil, err
		}
	}
	err := kafkaCfg.SetKey("group.id", gid)
	if err != nil {
		return nil, err
	}

	// Extract the consumer name from the client id.
	// We know by client.Consumer() that cid is in the form of `<group:name>|<topics>`
	cidNoTopics := strings.Split(strings.Split(string(cid), "|")[0], ":")[1]
	err = kafkaCfg.SetKey("client.id", cidNoTopics)
	if err != nil {
		return nil, err
	}

	ctx, consumerCancel := context.WithCancel(c.ctx)
	cons, err := NewConsumer(cid, topics, kafkaCfg)
	if err != nil {
		return nil, err
	}
	cons.cancel = consumerCancel

	c.consWg.Add(1)
	go func(ctx context.Context) {
		defer c.consWg.Done()
		cons.Run(ctx)
	}(ctx)

	return cons, nil
}

// Consumer method will either create a new consumer via the registerConsumer method, or it will
// simple return the already registered Consumer for the current Client.
func (c *Client) Consumer(topics []string, cfg rdkafka.ConfigMap) (*Consumer, error) {
	if !c.consReady {
		return nil, errors.New("Connection not ready. Identify yourself using `CLIENT SETNAME` first")
	}

	cid := ConsumerID(fmt.Sprintf("%s|%s", c.id, strings.Join(topics, ",")))

	if c.consumer == nil {
		cons, err := c.registerConsumer(cid, c.consGID, topics, cfg)
		if err != nil {
			return nil, fmt.Errorf("Failed to register Consumer '%s' for Client '%s'", cid, c.id)
		}
		c.consumer = cons
	} else if cid != c.consumer.id {
		return nil, fmt.Errorf("Client '%s' has Consumer '%s' registered (new consID: '%s')", c.id,
			c.consumer.id, cid)
	}

	return c.consumer, nil
}

// Producer returns c's producer. If c does not have a producer assigned yet,
// a new one is created and assigned to it.
func (c *Client) Producer(cfg rdkafka.ConfigMap) (*Producer, error) {
	var err error

	if c.producer != nil {
		return c.producer, nil
	}

	// reusing the same config between producers causes:
	// fatal error: concurrent map read and map write
	kafkaCfg := rdkafka.ConfigMap{}
	for k, v := range cfg {
		err := kafkaCfg.SetKey(k, v)
		if err != nil {
			c.log.Printf("Error configuring producer: %s", err)
		}
	}

	c.producer, err = NewProducer(kafkaCfg)
	if err != nil {
		return nil, err
	}

	return c.producer, nil
}

// Close closes producers, consumers and the connection of c.
// Calling Close on an already closed client will result in a panic.
func (c *Client) Close() {
	if c.consumer != nil {
		c.consumer.cancel()
	}

	if c.producer != nil {
		c.producer.Close()
	}

	close(c.monitorChan)
	c.conn.Close()
}
