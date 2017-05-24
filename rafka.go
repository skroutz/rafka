package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/urfave/cli"
)

// TODO(agis): This is not used anywhere. Either use it or get rid of it.
type Consumer interface {
	Messages() chan<- struct{}
	Run()
}

var kafkaCfg rdkafka.ConfigMap

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
		err = dec.Decode(&kafkaCfg)
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
	var managerWg sync.WaitGroup
	managerCtx, managerCancel := context.WithCancel(ctx)
	manager := NewConsumerManager(managerCtx)

	managerWg.Add(1)
	go func() {
		defer managerWg.Done()
		manager.Run()
	}()

	l.Println("Spawning server")
	var serverWg sync.WaitGroup
	serverCtx, serverCancel := context.WithCancel(ctx)
	rafka := NewServer(serverCtx, manager, 5*time.Second)

	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		err := rafka.ListenAndServe(":6380")
		if err != nil {
			panic(err)
		}

	}()

	<-sigCh
	l.Println("Received shutdown signal. Waiting for server to shutdown...")
	serverCancel()
	serverWg.Wait()

	l.Println("Waiting for consumer manager to shutdown...")
	managerCancel()
	managerWg.Wait()

	l.Println("Bye!")
}
