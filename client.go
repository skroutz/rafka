package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type Client struct {
	id          string
	consumerGID string
	manager     *ConsumerManager
	conn        net.Conn
	log         *log.Logger
	ready       bool

	consumers map[ConsumerID]bool
	byTopic   map[string]ConsumerID
}

func NewClient(conn net.Conn, cm *ConsumerManager) *Client {
	id := conn.RemoteAddr().String()

	return &Client{
		id:        id,
		manager:   cm,
		consumers: make(map[ConsumerID]bool),
		byTopic:   make(map[string]ConsumerID),
		conn:      conn,
		log:       log.New(os.Stderr, fmt.Sprintf("[client-%s] ", id), log.Ldate|log.Ltime)}
}

// SetID sets the id for c.
//
// It returns an error if id is not in the form of "<group.id>:<client-name>".
func (c *Client) SetID(id string) error {
	if c.ready {
		return errors.New("Client id is already set to " + c.id)
	}

	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return errors.New("Cannot parse group.id")
	}
	c.id = id
	c.consumerGID = parts[0]
	c.log.SetPrefix(fmt.Sprintf("[client-%s] ", id))
	c.ready = true

	return nil
}

func (c *Client) String() string {
	return c.id
}

func (c *Client) Consumer(topics []string) (*kafka.Consumer, error) {
	if !c.ready {
		return nil, errors.New("Connection not ready. Identify yourself using `CLIENT SETNAME` first")
	}

	consumerID := ConsumerID(fmt.Sprintf("%s|%s", c.id, strings.Join(topics, ",")))

	// Check for topics that already have a consumer
	for _, topic := range topics {
		if existingID, ok := c.byTopic[topic]; ok {
			if existingID != consumerID {
				return nil, fmt.Errorf("Topic %s has another consumer", topic)
			}
		}
	}

	// Register the Consumer
	c.consumers[consumerID] = true
	for _, topic := range topics {
		c.byTopic[topic] = consumerID
	}

	return c.manager.Get(consumerID, c.consumerGID, topics), nil
}

func (c *Client) ConsumerByTopic(topic string) (*kafka.Consumer, error) {
	consumerID, ok := c.byTopic[topic]
	if !ok {
		return nil, fmt.Errorf("No consumer for topic %s", topic)
	}

	consumer, err := c.manager.ByID(consumerID)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}
