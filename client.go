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
		return errors.New("CLient ID is already set to " + c.id)
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

func (c *Client) Consumer(topics []string) (*Consumer, error) {
	if !c.consReady {
		return nil, errors.New("Connection not ready. Identify yourself using `CLIENT SETNAME` first")
	}

	consID := ConsumerID(fmt.Sprintf("%s|%s", c.id, strings.Join(topics, ",")))

	for _, topic := range topics {
		if existingID, ok := c.consByTopic[topic]; ok {
			if existingID != consID {
				return nil, fmt.Errorf("Topic %s has another consumer", topic)
			}
		}
	}

	c.consumers[consID] = true
	for _, topic := range topics {
		c.consByTopic[topic] = consID
	}

	return c.consManager.Get(consID, c.consGID, topics), nil
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
func (c *Client) Producer(cfg *rdkafka.ConfigMap) (*Producer, error) {
	if c.producer != nil {
		return c.producer, nil
	}
	c.producer, err = rdkafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	return c.producer, nil

}

// Close closes c's underlying producers and consumers. Calling Close on
// an already closed client will result in a panic.
func (c *Client) Close() {
	for cid := range c.consumers {
		// TODO(agis): make this blocking
		c.consManager.ShutdownConsumer(cid)
	}

	if c.producer != nil {
		c.producer.Close()
	}
}
