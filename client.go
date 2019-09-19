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
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	redisproto "github.com/secmask/go-redisproto"
)

type Client struct {
	id          string
	conn        net.Conn
	log         *log.Logger
	monitorChan chan string

	consManager *ConsumerManager
	consID      ConsumerID
	consGID     string
	consReady   bool

	producer *Producer
}

// NewClient returns a new client. After it's no longer needed, the client
// should be closed with Close().
func NewClient(conn net.Conn, cm *ConsumerManager) *Client {
	id := conn.RemoteAddr().String()

	client := &Client{
		id:          id,
		monitorChan: make(chan string, 1000),
		conn:        conn,
		log:         log.New(os.Stderr, fmt.Sprintf("[client-%s] ", id), log.Ldate|log.Ltime),
		consManager: cm,
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

func (c *Client) Consumer(topics []string, cfg rdkafka.ConfigMap) (*Consumer, error) {
	if !c.consReady {
		return nil, errors.New("Connection not ready. Identify yourself using `CLIENT SETNAME` first")
	}

	// we include the topics in the consumer's id so that GetOrCreate()
	// creates a new consumer if the client issues a BLPOP for a different
	// topic
	cid := ConsumerID(fmt.Sprintf("%s|%s", c.id, strings.Join(topics, ",")))

	if c.consID == "" {
		c.consID = cid
	}

	if cid != c.consID {
		return nil, fmt.Errorf("Client '%s' has already Consumer '%s' registered.", c.id, c.consID)
	}

	return c.consManager.GetOrCreate(cid, c.consGID, topics, cfg)
}

// fetchConsumer returns the registered Consumer for the current Client.
func (c *Client) fetchConsumer() (*Consumer, error) {
	if c.consID == "" {
		return nil, errors.New("No consumer is registered for Client " + c.id)
	}

	consumer, err := c.consManager.ByID(c.consID)
	if err != nil {
		return nil, err
	}

	return consumer, nil
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
	c.consManager.ShutdownConsumer(c.consID)

	if c.producer != nil {
		c.producer.Close()
	}

	close(c.monitorChan)
	c.conn.Close()
}
