package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type consumerPool map[ConsumerID]*consumerPoolEntry

type ConsumerID string

type consumerPoolEntry struct {
	consumer *kafka.Consumer
	cancel   context.CancelFunc
}

type ConsumerManager struct {
	mu   sync.Mutex
	pool consumerPool

	log         *log.Logger
	consumersWg sync.WaitGroup
	ctx         context.Context
}

func NewConsumerManager(ctx context.Context) *ConsumerManager {
	return &ConsumerManager{
		log:  log.New(os.Stderr, "[manager] ", log.Ldate|log.Ltime),
		pool: make(consumerPool),
		ctx:  ctx,
	}
}

func (m *ConsumerManager) Run() {
	<-m.ctx.Done()
	m.log.Println("Waiting for all consumers to finish...")
	m.consumersWg.Wait()
	m.log.Println("All consumers shut down, bye!")
}

// Get returns a Kafka consumer associated with id, groupID and topics.
// It creates a new one if none exists.
func (m *ConsumerManager) Get(id ConsumerID, groupID string, topics []string) *kafka.Consumer {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.pool[id]; !ok {
		// Generate a new ConfigMap
		// Copying/reusing the same between consumers seems
		// to silently make the consumer non-operational.
		cfg := make(rdkafka.ConfigMap)
		for k, v := range kafkaCfg {
			cfg[k] = v
		}
		cfg.SetKey("group.id", groupID)

		c := kafka.NewConsumer(string(id), topics, &cfg)
		ctx, cancel := context.WithCancel(m.ctx)
		m.pool[id] = &consumerPoolEntry{
			consumer: c,
			cancel:   cancel,
		}

		m.log.Printf("Spawning Consumer %s config:%v", id, cfg)
		m.consumersWg.Add(1)
		go func(ctx context.Context) {
			defer m.consumersWg.Done()
			c.Run(ctx)
		}(ctx)
	}

	return m.pool[id].consumer
}

func (m *ConsumerManager) ByID(id ConsumerID) (*kafka.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.pool[id]
	if !ok {
		return nil, fmt.Errorf("No consumer with ConsumerID %s", id)
	}

	return entry.consumer, nil
}

// ShutdownConsumer signals the consumer denoted by id to shutdown. It returns
// false if no consumer was found.
//
// It does not block until the consumer is actually closed (this is instead
// done when ConsumerManager is closed).
func (m *ConsumerManager) ShutdownConsumer(id ConsumerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.pool[id]
	if !ok {
		return false
	}

	m.log.Printf("Shutting down consumer %s...", id)
	delete(m.pool, id)

	// We don't block waiting for the consumer to finish. Perhaps we should.
	//
	// To our defense, when the Manager is stopped it will be wait for all
	// consumers to gracefully stop due the m.wg WaitGroup.
	//
	// TODO we might end up with two consumers with the same
	// configration if a new Get() instanciates a new consumer
	// before the old one is destroyed. We need to investigate
	// if that's something to worry about. We could also make
	// consumer destroy sync and be done with it.
	c.cancel()

	return true
}
