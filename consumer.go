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
	out         chan *rdkafka.Message
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
		log:         log.New(os.Stderr, fmt.Sprintf("[consumer] [%s] ", id), log.Ldate|log.Ltime),
		out:         make(chan *rdkafka.Message),
		commitIntvl: commitIntvl,
		offsets:     make(map[TopicPartition]rdkafka.Offset),
		offsetIn:    make(chan OffsetEntry, 10000)}

	c.consumer, err = rdkafka.NewConsumer(&cfg)
	if err != nil {
		// TODO should be fatal?
		c.log.Fatal(err)
	}

	return &c
}

func (c *Consumer) Out() <-chan *rdkafka.Message {
	return c.out
}

func (c *Consumer) Run(ctx context.Context) {
	c.consumer.SubscribeTopics(c.topics, nil)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		c.run(ctx)
	}(ctx)

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

	wg.Wait()
	c.log.Println("Bye!")
}

func (c *Consumer) run(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			c.log.Println("Closing consumer...")
			c.log.Printf("Unhandled (no worries) kafka Events(): %d", len(c.consumer.Events()))
			err := c.consumer.Close()
			if err != nil {
				c.log.Printf("Error closing: %s", err)
			}
			break Loop
		case ev := <-c.consumer.Events():
			switch e := ev.(type) {
			case rdkafka.AssignedPartitions:
				c.log.Print(e)
				c.consumer.Assign(e.Partitions)
			case rdkafka.RevokedPartitions:
				c.log.Print(e)
				c.consumer.Unassign()
			case *rdkafka.Message:
				// We cannot block on c.out, we need to make sure
				// that we ctx.Done() is propagated correctly.
				select {
				case <-ctx.Done():
					// Just swallow it, we just need to unblock.
					// the Done() will be dealt in the top level
					// select {}.
				case c.out <- e:
				}
			case rdkafka.PartitionEOF:
				// nothing to do in this case
			case rdkafka.Error:
				// TODO Handle gracefully?
				c.log.Printf("%% Error: %v\n", e)
				break Loop
			}
		}
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
			c.log.Printf("Error committing offset %+v: %s", off, err)
			continue
		}
		c.log.Printf("Commited offset %#v", rdkafkaTp)
		delete(c.offsets, tp)
	}
}
