package main

import (
	"context"
	"fmt"
	"log"
	"os"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	ctx    context.Context
	cancel func()

	done   chan struct{}
	rdProd *rdkafka.Producer
	log    *log.Logger
}

func NewProducer(ctx context.Context, cfg *rdkafka.ConfigMap) (*Producer, error) {
	rdProd, err := rdkafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	newCtx, cancelFn := context.WithCancel(ctx)

	return &Producer{
		ctx:    newCtx,
		cancel: cancelFn,
		rdProd: rdProd,
		log:    log.New(os.Stderr, fmt.Sprintf("[%s] ", rdProd), log.Ldate|log.Ltime),
		done:   make(chan struct{})}, nil

}

// Run logs message delivery failures and also closes p when ctx is
// cancelled.
func (p *Producer) Run() {
	for {
		select {
		case <-p.ctx.Done():
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
			p.done <- struct{}{}
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

// Close stops p's Run()
func (p *Producer) Close() {
	p.cancel()
	<-p.done
	p.log.Print("Bye")
}
