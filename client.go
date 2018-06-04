// Copyright 2017 Skroutz S.A.
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
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client struct {
	id   string
	conn net.Conn
	log  *log.Logger

	consManager *ConsumerManager
	consGID     string
	consReady   bool
	consByTopic map[string]ConsumerID
	consumers   map[ConsumerID]bool

	producer *Producer
}

// NewClient returns a new client. After it's no longer needed, the client
// should be closed with Close().
func NewClient(conn net.Conn, cm *ConsumerManager) *Client {
	id := conn.RemoteAddr().String()
	return &Client{
		id:          id,
		conn:        conn,
		log:         log.New(os.Stderr, fmt.Sprintf("[client-%s] ", id), log.Ldate|log.Ltime),
		consManager: cm,
		consumers:   make(map[ConsumerID]bool),
		consByTopic: make(map[string]ConsumerID),
	}
}

// SetID sets the id for c. It returns an error if id is not in the form
// "<consumer-group>:<consumer-id>".
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
	c.log.SetPrefix(fmt.Sprintf("[client-%s] ", id))
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

	for _, topic := range topics {
		if existingID, ok := c.consByTopic[topic]; ok {
			if existingID != cid {
				return nil, fmt.Errorf("Topic %s has another consumer", topic)
			}
		}
	}

	c.consumers[cid] = true
	for _, topic := range topics {
		c.consByTopic[topic] = cid
	}

	return c.consManager.GetOrCreate(cid, c.consGID, topics, cfg)
}

func (c *Client) ConsumerByTopic(topic string) (*Consumer, error) {
	consumerID, ok := c.consByTopic[topic]
	if !ok {
		return nil, fmt.Errorf("No consumer for topic %s", topic)
	}

	consumer, err := c.consManager.ByID(consumerID)
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
	for cid := range c.consumers {
		// TODO(agis): make this blocking
		c.consManager.ShutdownConsumer(cid)
	}

	if c.producer != nil {
		c.producer.Close()
	}

	c.conn.Close()
}
