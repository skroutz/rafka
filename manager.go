package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type ConsumerID string
type consumerPoolEntry struct {
	consumer *kafka.Consumer
	cancel   context.CancelFunc
}

type consumerPool map[ConsumerID]*consumerPoolEntry

type Manager struct {
	pool consumerPool
	log  *log.Logger
	wg   sync.WaitGroup
	mu   sync.Mutex
	ctx  context.Context
}

func NewManager(ctx context.Context) *Manager {
	c := Manager{
		log:  log.New(os.Stderr, "[manager] ", log.Ldate|log.Ltime),
		pool: make(consumerPool),
		ctx:  ctx,
	}

	return &c
}

func (m *Manager) Run() {
	tickGC := time.NewTicker(10 * time.Second)
	defer tickGC.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.log.Println("Shutting down...")
			m.cleanup()
			return
		case <-tickGC.C:
			m.reapStale()
		}
	}
}

func (m *Manager) Get(id ConsumerID, groupID string, topics []string) *kafka.Consumer {
	// Create consumer if it doesn't exist
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pool[id]; !ok {
		// Generate a new ConfigMap
		// Copying/reusing the same between consumers seems
		// to silently make the consumer non-operational.
		cfg := make(rdkafka.ConfigMap)
		for k, v := range kafkacfg {
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
		m.wg.Add(1)
		go func(ctx context.Context) {
			defer m.wg.Done()
			c.Run(ctx)
		}(ctx)
	}

	return m.pool[id].consumer
}

func (m *Manager) ByID(id ConsumerID) (*kafka.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.pool[id]
	if !ok {
		return nil, fmt.Errorf("No consumer with ConsumerID %s", id)
	}

	return entry.consumer, nil
}

func (m *Manager) Delete(id ConsumerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.pool[id]
	if !ok { // No consumer with that ID
		return false
	}

	delete(m.pool, id)
	// We don't block waiting for the consumer to finish. Perhaps we should.
	//
	// To our defense, when the Manager is stopped it will be wait for all
	// consumers to gracefully stop due the m.wg WaitGroup.

	// TODO we might end up with two consumers with the same
	// configration if a new Get() instanciates a new consumer
	// before the old one is destroyed. We need to investigate
	// if that's something to worry about. We could also make
	// consumer destroy sync and be done with it.
	e.cancel()
	return true
}

func (m *Manager) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, entry := range m.pool {
		m.log.Printf("Terminating consumer %s", id)
		entry.cancel()
	}

	m.log.Println("Waiting all consumers to finish...")
	m.wg.Wait()
	m.log.Println("All consumers shut down, bye!")
}

func (m *Manager) reapStale() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, _ := range m.pool {
		// TODO actual cleanup
		m.log.Printf("Cleaning up: %s", id)
	}
	return
}
