package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	id       string
	consumer *rdkafka.Consumer
	topics   []string
	out      chan *rdkafka.Message
	log      *log.Logger
}

func NewConsumer(id string, topics []string, cfg *rdkafka.ConfigMap) *Consumer {
	var err error

	c := Consumer{
		id:     id,
		topics: topics,
		log:    log.New(os.Stderr, fmt.Sprintf("[consumer] [%s] ", id), log.Ldate|log.Ltime),
		out:    make(chan *rdkafka.Message)}

	c.consumer, err = rdkafka.NewConsumer(cfg)
	if err != nil {
		// TODO should be fatal?
		c.log.Fatal(err)
	}

	return &c
}

func (c *Consumer) Out() <-chan *rdkafka.Message {
	return c.out
}

func (c *Consumer) CommitOffset(topic string, partition int32, offset int64) error {
	tp := rdkafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    rdkafka.Offset(offset)}

	offsets := []rdkafka.TopicPartition{tp}
	// This is the message offset, we need to point the consumer to the next
	// message.
	offsets[0].Offset++

	c.log.Printf("Committing: %+v", offsets)
	committed, err := c.consumer.CommitOffsets(offsets)
	if err != nil {
		c.log.Printf("Error committing offsets %+v, %s", offset, err)
		return err
	}
	c.log.Printf("Committed: %+v", committed)

	return nil
}

func (c *Consumer) Run(ctx context.Context) {
	c.consumer.SubscribeTopics(c.topics, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		c.run(ctx)
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
				c.log.Printf("%% %v\n", e)
				c.consumer.Assign(e.Partitions)
			case rdkafka.RevokedPartitions:
				c.log.Printf("%% %v\n", e)
				c.consumer.Unassign()
			case *rdkafka.Message:
				msg := string(e.Value)
				// We cannot block on c.out, we need to make sure
				// that we ctx.Done() is propagated correctly.
				select {
				case <-ctx.Done():
					// Just swallow it, we just need to unblock.
					// the Done() will be dealt in the top level
					// select {}.
				case c.out <- e:
					c.log.Printf("%% Message on %s:\n%s\n",
						e.TopicPartition, msg)
				}
			case rdkafka.PartitionEOF:
				// TODO
				// c.log.Printf("%% Reached %v\n", e)
			case rdkafka.Error:
				// TODO Handle gracefully?
				c.log.Printf("%% Error: %v\n", e)
				break Loop
			}
		}
	}
}
