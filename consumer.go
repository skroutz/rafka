// Copyright 2017-2019 Skroutz S.A.
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

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerID string

type Consumer struct {
	id       ConsumerID
	consumer *rdkafka.Consumer
	topics   []string
	cfg      rdkafka.ConfigMap
	log      *log.Logger

	mu         *sync.Mutex
	terminated bool
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

type OffsetEntry struct {
	tp     TopicPartition
	offset rdkafka.Offset
}

func NewConsumer(id ConsumerID, topics []string, cfg rdkafka.ConfigMap) (*Consumer, error) {
	var err error

	c := Consumer{
		id:     id,
		topics: topics,
		cfg:    cfg,
		log:    log.New(os.Stderr, fmt.Sprintf("[consumer-%s] ", id), log.Ldate|log.Ltime),
		mu:     new(sync.Mutex),
	}

	c.consumer, err = rdkafka.NewConsumer(&cfg)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Consumer) Run(ctx context.Context) {
	c.consumer.SubscribeTopics(c.topics, nil)

	c.log.Printf("Started working (%v)...", c.cfg)
	<-ctx.Done()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Close() will also trigger a commit since enable.auto.commit is true
	// so we don't need to commit explicitly
	err := c.consumer.Close()
	if err != nil {
		c.log.Printf("Error closing: %s", err)
	}
	c.terminated = true
	c.log.Println("Bye")
}

// filterOutInvalidOffsets filters out any Offsets of type OffsetInvalid from the given OffsetsCommitted argument.
func filterOutInvalidOffsets(offsets rdkafka.OffsetsCommitted) rdkafka.OffsetsCommitted {
	updatedParts := []rdkafka.TopicPartition{}
	for _, part := range offsets.Offsets {
		if part.Offset != rdkafka.OffsetInvalid {
			updatedParts = append(updatedParts, part)
		}
	}
	return rdkafka.OffsetsCommitted{offsets.Error, updatedParts}
}

func (c *Consumer) Poll(timeoutMS int) (*rdkafka.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.terminated {
		return nil, nil
	}

	ev := c.consumer.Poll(timeoutMS)
	if ev == nil {
		return nil, nil
	}

	switch e := ev.(type) {
	case *rdkafka.Message:
		return e, nil
	case rdkafka.OffsetsCommitted:
		c.log.Print(filterOutInvalidOffsets(e))
	case rdkafka.Error:
		// Treat errors with error code ErrTransport as transient And just log them.
		// For now all other errors cause a failure.
		// https://github.com/edenhill/librdkafka/issues/1987#issuecomment-422008750
		if e.Code() != rdkafka.ErrTransport {
			return nil, errors.New(e.String())
		}

		c.log.Printf("Consumer: Poll: Transient error: %s , code: %d", e.String(), e.Code())
	default:
		c.log.Printf("Unknown event type: %T", e)
	}

	return nil, nil
}

// SetOffset sets the offset for the given topic and partition to pos.
// Commiting the offset to Kafka is handled by librdkafka in the background.
func (c *Consumer) SetOffset(topic string, partition int32, pos rdkafka.Offset) error {
	if pos < 0 {
		return fmt.Errorf("offset cannot be negative, got %d", pos)
	}

	// Calling StoreOffsets manually prohibits the caller from using
	// `enable.auto.offset.store` option.
	// See https://github.com/edenhill/librdkafka/blob/v0.11.4/src/rdkafka.h#L2665
	_, err := c.consumer.StoreOffsets([]rdkafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: partition,
			Offset:    pos,
		},
	})
	return err
}
