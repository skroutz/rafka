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
	rc := Conn{
		manager: manager,
		used:    make(map[ConsumerID]bool),
		byTopic: make(map[string]ConsumerID),
		log:     log.New(os.Stderr, "[client] ", log.Ldate|log.Ltime),
		ready:   false,
	}

	return &rc
}

func (rc *Conn) SetID(id string) error {
	rc.id = id
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return errors.New("Cannot parse group.id")
	}
	rc.groupID = parts[0]
	rc.ready = true

	return nil
}

func (rc *Conn) String() string {
	return string(rc.id)
}

func (rc *Conn) Consumer(topics []string) (*kafka.Consumer, error) {
	if !rc.ready {
		return nil, errors.New("Connection is not ready, please identify before using")
	}

	consumerID := ConsumerID(fmt.Sprintf("%s|%s", rc.id, strings.Join(topics, ",")))

	// Check for topics that already have a consumer
	for _, topic := range topics {
		if existingID, ok := rc.byTopic[topic]; ok {
			if existingID != consumerID {
				return nil, fmt.Errorf("Topic %s is already consumed", topic)
			}
		}
	}

	// Register the Consumer
	rc.used[consumerID] = true
	for _, topic := range topics {
		rc.byTopic[topic] = consumerID
	}

	return rc.manager.Get(consumerID, rc.groupID, topics), nil
}

func (rc *Conn) ConsumerByTopic(topic string) (*kafka.Consumer, error) {
	consumerID, ok := rc.byTopic[topic]
	if !ok {
		return nil, fmt.Errorf("No consumer for topic %s", topic)
	}

	consumer, err := rc.manager.ByID(consumerID)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (rc *Conn) Teardown() {
	for cid := range rc.used {
		rc.log.Printf("[%s] Scheduling teardown for %s", rc.id, cid)
		rc.manager.Delete(cid)
	}
}
