package main

import (
	"fmt"
	"log"
	"os"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	rdProd    *rdkafka.Producer
	log       *log.Logger
	monitored bool

	close, done chan struct{}
}

func NewProducer(cfg *rdkafka.ConfigMap) (*Producer, error) {
	rdProd, err := rdkafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{
		close:     make(chan struct{}),
		rdProd:    rdProd,
		log:       log.New(os.Stderr, fmt.Sprintf("[%s] ", rdProd), log.Ldate|log.Ltime),
		monitored: false,
		done:      make(chan struct{})}, nil

}

func (p *Producer) Produce(msg *rdkafka.Message) error {
	if !p.monitored {
		go p.monitor()
		p.monitored = true
	}

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

// Close stops p after flushing any buffered messages. It is a blocking
// operation.
func (p *Producer) Close() {
	p.close <- struct{}{}
	<-p.done
	p.log.Print("Bye!")
}

// monitor logs message delivery failures. It also cleans up when p is closed.
func (p *Producer) monitor() {
	for {
		select {
		case <-p.close:
			p.log.Print("Flushing and shutting down...")
			// TODO(agis) make this configurable?
			unflushed := p.rdProd.Flush(5000)
			if unflushed > 0 {
				p.log.Printf("Flush timeout: %d unflushed events", unflushed)
			} else {
				p.log.Printf("All messages flushed")
			}
			select {
			case ev := <-p.rdProd.Events():
				msg := ev.(*rdkafka.Message)
				if err := msg.TopicPartition.Error; err != nil {
					p.log.Printf("Failed to deliver %v", msg)
				}
			default:
			}
			p.rdProd.Close()
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
