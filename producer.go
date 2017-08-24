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
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	id      string
	rdProd  *rdkafka.Producer
	log     *log.Logger
	started bool
	wg      sync.WaitGroup
}

func NewProducer(cfg *rdkafka.ConfigMap) (*Producer, error) {
	rdProd, err := rdkafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	id := strings.TrimPrefix(rdProd.String(), "rdkafka#")

	return &Producer{
		id:      id,
		rdProd:  rdProd,
		log:     log.New(os.Stderr, fmt.Sprintf("[%s] ", id), log.Ldate|log.Ltime),
		started: false}, nil

}

func (p *Producer) String() string {
	return p.id
}

func (p *Producer) Produce(msg *rdkafka.Message) error {
	if !p.started {
		p.wg.Add(1)
		go p.consumeDeliveries()
		p.started = true
		p.log.Printf("Started working...")
	}

	return p.rdProd.Produce(msg, nil)
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
	unflushed := p.rdProd.Flush(5000)
	if unflushed > 0 {
		p.log.Printf("Flush timeout: %d unflushed events", unflushed)
	}
	// signal consumeDeliveries() to exit by closing p.rdProd.Events()
	// channel
	p.rdProd.Close()
	p.wg.Wait()
	p.log.Print("Bye")
}

func (p *Producer) consumeDeliveries() {
	defer p.wg.Done()

	for ev := range p.rdProd.Events() {
		msg := ev.(*rdkafka.Message)
		if err := msg.TopicPartition.Error; err != nil {
			p.log.Printf("Failed to deliver `%s` to %s: %s", msg.Value, msg, err)
		}
	}
}
