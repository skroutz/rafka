package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	rdProd *rdkafka.Producer
	log    *log.Logger
}

func NewProducer(cfg *rdkafka.ConfigMap) (*Producer, error) {
	rdProd, err := rdkafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{
		rdProd: rdProd,
		log:    log.New(os.Stderr, fmt.Sprintf("[%s] ", rdProd), log.Ldate|log.Ltime)}, nil

}

// Monitor logs message delivery failures and also closes p when ctx is
// cancelled.
func (p *Producer) Monitor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			p.log.Print("Flushing and shutting down...")
			// TODO(agis) make this configurable?
			unflushed := p.rdProd.Flush(5000)
			if unflushed > 0 {
				p.log.Printf("Flush timeout: %d unflushed events", unflushed)
			}
			p.rdProd.Close()
			for ev := range p.rdProd.Events() {
				msg := ev.(*rdkafka.Message)
				if err := msg.TopicPartition.Error; err != nil {
					p.log.Printf("Failed to deliver %v", msg)
				}
			}
			p.log.Print("Bye")
			return
		case ev := <-p.rdProd.Events():
			msg := ev.(*rdkafka.Message)
			if err := msg.TopicPartition.Error; err != nil {
				p.log.Printf("Failed to deliver %v", msg)
			}
		}
	}
}

func (p *Producer) Produce(msg *rdkafka.Message) error {
	err := p.rdProd.Produce(msg, nil)
	if err != nil {
		return err
	}
	return nil
}

// Flush flushes any remaining messages. It blocks until all messages are
// delivered or timeoutMs elapses and returns the number of
// outstanding messages.
func (p *Producer) Flush(timeoutMs int) int {
	return p.rdProd.Flush(timeoutMs)

}
