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
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	id          string
	consumer    *rdkafka.Consumer
	topics      []string
	log         *log.Logger
	commitIntvl time.Duration

	offsets  map[TopicPartition]rdkafka.Offset
	offsetIn chan OffsetEntry
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

type OffsetEntry struct {
	tp     TopicPartition
	offset rdkafka.Offset
}

func NewConsumer(id string, topics []string, commitIntvl time.Duration, cfg rdkafka.ConfigMap) *Consumer {
	var err error

	c := Consumer{
		id:          id,
		topics:      topics,
		log:         log.New(os.Stderr, fmt.Sprintf("[consumer-%s] ", id), log.Ldate|log.Ltime),
		commitIntvl: commitIntvl,
		offsets:     make(map[TopicPartition]rdkafka.Offset),
		offsetIn:    make(chan OffsetEntry, 10000)}

	c.consumer, err = rdkafka.NewConsumer(&cfg)
	if err != nil {
		// TODO(agis): make this a log output instead if we ever accept
		// config from clients
		c.log.Fatal(err)
	}

	return &c
}

func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup

	c.consumer.SubscribeTopics(c.topics, nil)

	// offset commit goroutine
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		t := time.NewTicker(c.commitIntvl * time.Second)
		defer t.Stop()
	Loop:
		for {
			select {
			case <-ctx.Done():
				c.commitOffsets()
				break Loop
			case oe := <-c.offsetIn:
				c.offsets[oe.tp] = oe.offset
			case <-t.C:
				c.commitOffsets()
			}
		}
	}(ctx)

	c.log.Printf("Started working...")
	wg.Wait()
	err := c.consumer.Close()
	if err != nil {
		c.log.Printf("Error closing: %s", err)
	}
	c.log.Println("Bye")
}

func (c *Consumer) Poll(timeoutMS int) (*rdkafka.Message, error) {
	ev := c.consumer.Poll(timeoutMS)
	if ev == nil {
		return nil, nil
	}

	switch e := ev.(type) {
	case *rdkafka.Message:
		return e, nil
	case rdkafka.Error:
		return nil, errors.New(e.String())
	default:
		return nil, errors.New(fmt.Sprintf("Unknown event type: %v", e))
	}
}

// SetOffset sets the offset for the given topic and partition to pos.
// It does not actually commit the offset to Kafka (this is done periodically
// in the background).
func (c *Consumer) SetOffset(topic string, partition int32, pos rdkafka.Offset) error {
	if pos < 0 {
		return errors.New(fmt.Sprintf("offset cannot be negative, got %d", pos))
	}
	c.offsetIn <- OffsetEntry{TopicPartition{topic, partition}, pos}
	return nil
}

// commitOffsets commits the offsets stored in c to Kafka.
func (c *Consumer) commitOffsets() {
	for tp, off := range c.offsets {
		rdkafkaTp := []rdkafka.TopicPartition{{
			Topic:     &tp.Topic,
			Partition: tp.Partition,
			Offset:    off}}

		_, err := c.consumer.CommitOffsets(rdkafkaTp)
		if err != nil {
			c.log.Printf("Error committing offset %d for %s: %s", off, rdkafkaTp, err)
			continue
		}
		delete(c.offsets, tp)
	}
}
