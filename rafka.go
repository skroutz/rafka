package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/urfave/cli"
)

type Consumer interface {
	Messages() chan<- struct{}
	Run()
}

var kafkacfg kafka.ConfigMap

func main() {
	app := cli.NewApp()
	app.Name = "rafka"
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "consumer,c",
			Usage: "Consumer type (kafka,dummy)",
		},
		cli.StringFlag{
			Name:  "kafka,k",
			Value: "",
			Usage: "librdkafka configuration `FILE`",
		},
	}

	app.Before = func(c *cli.Context) error {
		if c.String("kafka") == "" {
			return cli.NewExitError("No kafka configuration!", 1)
		}

		f, err := os.Open(c.String("kafka"))
		if err != nil {
			return err
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		dec.UseNumber()
		err = dec.Decode(&kafkacfg)
		if err != nil {
			return err
		}

		return nil
	}

	app.Action = func(c *cli.Context) error {
		run(c)
		return nil
	}

	app.Run(os.Args)
}

func run(c *cli.Context) {
	l := log.New(os.Stderr, "[rafka] ", log.Ldate|log.Ltime)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx := context.Background()

	l.Println("Spawning Consumer Manager")
	var manager_wg sync.WaitGroup
	manager_ctx, manager_cancel := context.WithCancel(ctx)
	manager := NewManager(manager_ctx)

	manager_wg.Add(1)
	go func() {
		defer manager_wg.Done()
		manager.Run()
	}()

	l.Println("Spawning Redis server")
	var redis_wg sync.WaitGroup
	redis_ctx, redis_cancel := context.WithCancel(ctx)
	redis_server := NewRedisServer(redis_ctx, manager)

	redis_wg.Add(1)
	go func() {
		defer redis_wg.Done()
		err := redis_server.ListenAndServe(":6380")
		if err != nil {
			panic(err)
		}

	}()

	select {
	case <-sigCh:
		l.Println("Received shutdown signal..")
	}

	l.Println("Waiting for redis server to shutdown...")
	redis_cancel()
	redis_wg.Wait()

	l.Println("Waiting for consumer manager to shutdown...")
	manager_cancel()
	manager_wg.Wait()

	l.Println("Bye!")
}
