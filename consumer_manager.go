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
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type consumerPool map[ConsumerID]*consumerPoolEntry
type ConsumerID string
type consumerPoolEntry struct {
	consumer *Consumer
	cancel   context.CancelFunc
}

type ConsumerManager struct {
	mu   sync.Mutex
	pool consumerPool

	log         *log.Logger
	consumersWg sync.WaitGroup
	ctx         context.Context
	cfg         Config
}

func NewConsumerManager(ctx context.Context, cfg Config) *ConsumerManager {
	return &ConsumerManager{
		log:  log.New(os.Stderr, "[manager] ", log.Ldate|log.Ltime),
		pool: make(consumerPool),
		ctx:  ctx,
		cfg:  cfg}
}

func (m *ConsumerManager) Run() {
	<-m.ctx.Done()
	m.log.Println("Waiting for all consumers to finish...")
	m.consumersWg.Wait()
	m.log.Println("All consumers shut down. Bye")
}

// GetOrCreate returns the Consumer denoted by cid. If such a Consumer does not
// exist, a new one is created.
func (m *ConsumerManager) GetOrCreate(cid ConsumerID, gid string, topics []string) *Consumer {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.pool[cid]; !ok {
		// apparently, reusing the same config between consumers
		// silently makes them non-operational
		kafkaCfg := rdkafka.ConfigMap{}
		for k, v := range m.cfg.Librdkafka.Consumer {
			err := kafkaCfg.SetKey(k, v)
			if err != nil {
				m.log.Printf("Error configuring consumer: %s", err)
			}
		}
		err := kafkaCfg.SetKey("group.id", gid)
		if err != nil {
			m.log.Printf("Error configuring consumer: %s", err)
		}

		// Extract the consumer name from the client id.
		// We know by client.Consumer() that cid is in the form of
		// "<group:name>|<topics>"
		cidNoTopics := strings.Split(strings.Split(string(cid), "|")[0], ":")[1]
		err = kafkaCfg.SetKey("client.id", cidNoTopics)
		if err != nil {
			m.log.Printf("Error configuring consumer: %s", err)
		}

		c := NewConsumer(string(cid), topics, kafkaCfg)
		ctx, cancel := context.WithCancel(m.ctx)
		m.pool[cid] = &consumerPoolEntry{
			consumer: c,
			cancel:   cancel,
		}

		m.consumersWg.Add(1)
		go func(ctx context.Context) {
			defer m.consumersWg.Done()
			c.Run(ctx)
		}(ctx)
	}

	return m.pool[cid].consumer
}

func (m *ConsumerManager) ByID(cid ConsumerID) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.pool[cid]
	if !ok {
		return nil, fmt.Errorf("No consumer with id %s", cid)
	}

	return entry.consumer, nil
}

// ShutdownConsumer signals the consumer denoted by id to shutdown. It returns
// false if no consumer was found.
//
// It does not block until the consumer is actually closed (this is instead
// done when ConsumerManager is closed).
func (m *ConsumerManager) ShutdownConsumer(cid ConsumerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.pool[cid]
	if !ok {
		return false
	}

	delete(m.pool, cid)

	// We don't block waiting for the consumer to finish. Perhaps we should.
	//
	// To our defense, when the Manager is stopped it will be wait for all
	// consumers to gracefully stop due the m.wg WaitGroup.
	//
	// TODO we might end up with two consumers with the same
	// configuration if a new Get() instasiates a new consumer
	// before the old one is destroyed. We need to investigate
	// if that's something to worry about. We could also make
	// consumer destroy sync and be done with it.
	c.cancel()

	return true
}
