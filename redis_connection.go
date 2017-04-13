package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type clientID string
type ConsumerIDs map[ConsumerID]bool

type RedisConnection struct {
	id      clientID
	groupID string
	manager *Manager
	used    ConsumerIDs
	log     *log.Logger
	ready   bool
}

func NewRedisConnection(manager *Manager) *RedisConnection {
	rc := RedisConnection{
		manager: manager,
		used:    make(ConsumerIDs),
		log:     log.New(os.Stderr, "[redis-connection] ", log.Ldate|log.Ltime),
		ready:   false,
	}

	return &rc
}

func (rc *RedisConnection) SetID(id string) error {
	rc.id = clientID(id)
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return errors.New("Cannot parse group.id")
	}
	rc.groupID = parts[0]
	rc.ready = true

	return nil
}

func (rc *RedisConnection) String() string {
	return string(rc.id)
}

func (rc *RedisConnection) Consumer(topics []string) (*kafka.Consumer, error) {
	if !rc.ready {
		return nil, errors.New("Connection is not ready, please identify before using")
	}

	consumerID := ConsumerID(fmt.Sprintf("%s|%s", rc.id, strings.Join(topics, ",")))
	rc.used[consumerID] = true
	return rc.manager.Get(consumerID, rc.groupID, topics), nil
}

func (rc *RedisConnection) Teardown() {
	for cid, _ := range rc.used {
		rc.log.Printf("[%s] Scheduling teardown for %s", rc.id, cid)
		rc.manager.Delete(cid)
	}
}
