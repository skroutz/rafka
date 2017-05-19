package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type Conn struct {
	id      string
	groupID string
	manager *Manager
	used    map[ConsumerID]bool
	byTopic map[string]ConsumerID
	log     *log.Logger
	ready   bool
}

func NewConn(manager *Manager) *Conn {
	c := Conn{
		manager: manager,
		used:    make(map[ConsumerID]bool),
		byTopic: make(map[string]ConsumerID),
		log:     log.New(os.Stderr, "[client] ", log.Ldate|log.Ltime),
		ready:   false,
	}

	return &c
}

func (c *Conn) SetID(id string) error {
	c.id = id
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return errors.New("Cannot parse group.id")
	}
	c.groupID = parts[0]
	c.ready = true

	return nil
}

func (c *Conn) String() string {
	return string(c.id)
}

func (c *Conn) Consumer(topics []string) (*kafka.Consumer, error) {
	if !c.ready {
		return nil, errors.New("Connection is not ready, please identify before using")
	}

	consumerID := ConsumerID(fmt.Sprintf("%s|%s", c.id, strings.Join(topics, ",")))

	// Check for topics that already have a consumer
	for _, topic := range topics {
		if existingID, ok := c.byTopic[topic]; ok {
			if existingID != consumerID {
				return nil, fmt.Errorf("Topic %s is already consumed", topic)
			}
		}
	}

	// Register the Consumer
	c.used[consumerID] = true
	for _, topic := range topics {
		c.byTopic[topic] = consumerID
	}

	return c.manager.Get(consumerID, c.groupID, topics), nil
}

func (c *Conn) ConsumerByTopic(topic string) (*kafka.Consumer, error) {
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

func (c *Conn) Teardown() {
	for cid := range c.used {
		c.log.Printf("[%s] Scheduling teardown for %s", c.id, cid)
		c.manager.Delete(cid)
	}
}
