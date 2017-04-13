package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	id       string
	consumer *kafka.Consumer
	topics   []string
	out      chan string
	log      *log.Logger
}

func NewConsumer(id string, topics []string, cfg *kafka.ConfigMap) *Consumer {
	var err error

	log_prefix := fmt.Sprintf("[consumer] [%s] ", id)
	c := Consumer{
		id:     id,
		topics: topics,
		log:    log.New(os.Stderr, log_prefix, log.Ldate|log.Ltime),
		out:    make(chan string)}

	c.consumer, err = kafka.NewConsumer(cfg)
	if err != nil {
		// TODO should be fatal?
		c.log.Fatal(err)
	}

	return &c
}

func (c *Consumer) Out() <-chan string {
	return c.out
}

func (c *Consumer) Ack(topic string, partition int32, offset int64) error {
	tp := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset)}

	offsets := []kafka.TopicPartition{tp}
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
			c.consumer.Close()
			break Loop
		case ev := <-c.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.log.Printf("%% %v\n", e)
				c.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.log.Printf("%% %v\n", e)
				c.consumer.Unassign()
			case *kafka.Message:
				msg := string(e.Value)
				// We cannot block on c.out, we need to make sure
				// that we ctx.Done() is propagated correctly.
				select {
				case <-ctx.Done():
					// Just swallow it, we just need to unblock.
					// the Done() will be dealt in the top level
					// select {}.
				case c.out <- msg:
					c.log.Printf("%% Message on %s:\n%s\n",
						e.TopicPartition, msg)
				}
			case kafka.PartitionEOF:
				// TODO
				// c.log.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// TODO Handle gracefully?
				c.log.Printf("%% Error: %v\n", e)
				break Loop
			}
		}
	}
}
