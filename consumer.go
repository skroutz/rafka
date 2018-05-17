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

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	id       string
	consumer *rdkafka.Consumer
	topics   []string
	log      *log.Logger
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

type OffsetEntry struct {
	tp     TopicPartition
	offset rdkafka.Offset
}

func NewConsumer(id string, topics []string, cfg rdkafka.ConfigMap) *Consumer {
	var err error

	c := Consumer{
		id:     id,
		topics: topics,
		log:    log.New(os.Stderr, fmt.Sprintf("[consumer-%s] ", id), log.Ldate|log.Ltime),
	}

	c.consumer, err = rdkafka.NewConsumer(&cfg)
	if err != nil {
		// TODO(agis): make this a log output instead if we ever accept
		// config from clients
		c.log.Fatal(err)
	}

	return &c
}

func (c *Consumer) Run(ctx context.Context) {
	c.consumer.SubscribeTopics(c.topics, nil)

	c.log.Printf("Started working...")
	<-ctx.Done()

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
		c.log.Printf("Unknown event type: %T", e)
		return nil, nil
	}
}

// SetOffset sets the offset for the given topic and partition to pos.
// Commiting the offset to Kafka is handled by librdkafka in the background.
func (c *Consumer) SetOffset(topic string, partition int32, pos rdkafka.Offset) error {
	if pos < 0 {
		return fmt.Errorf("offset cannot be negative, got %d", pos)
	}
	_, err := c.consumer.StoreOffsets([]rdkafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: partition,
			Offset:    pos,
		},
	})
	return err
}
