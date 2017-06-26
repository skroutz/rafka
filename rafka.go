package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/urfave/cli"
)

var cfg Config

func main() {
	app := cli.NewApp()
	app.Name = "rafka"
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "kafka, k",
			Usage: "Load librdkafka configuration from `FILE`",
		},
		cli.Int64Flag{
			Name:  "commit-intvl, i",
			Usage: "Commit offsets of each consumer every `N` seconds",
			Value: 10,
		},
	}

	app.Before = func(c *cli.Context) error {
		if c.String("kafka") == "" {
			return cli.NewExitError("No librdkafka configuration provided!", 1)
		}

		f, err := os.Open(c.String("kafka"))
		if err != nil {
			return err
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		dec.UseNumber()
		err = dec.Decode(&cfg.Librdkafka)
		if err != nil {
			return err
		}

		if c.Int64("commit-intvl") <= 0 {
			return errors.New("`commit-intvl` option must be greater than 0")
		}
		cfg.CommitIntvl = time.Duration(c.Int64("commit-intvl"))

		// republish config using rdkafka.SetKey() for proper error checking
		for _, config := range []rdkafka.ConfigMap{cfg.Librdkafka.Consumer, cfg.Librdkafka.Producer} {
			// merge general configuration
			for k, v := range cfg.Librdkafka.General {
				if config[k] != nil {
					continue
				}
				err = config.SetKey(k, v)
				if err != nil {
					return errors.New(fmt.Sprintf("Error in librdkafka config (%s): %s", k, err))
				}
			}

			for k, v := range config {
				err = config.SetKey(k, v)
				if err != nil {
					return errors.New(fmt.Sprintf("Error in librdkafka config (%s): %s", k, err))
				}
			}
		}

		if cfg.Librdkafka.Consumer["go.events.channel.size"] != nil {
			chSizeNumber, ok := cfg.Librdkafka.Consumer["go.events.channel.size"].(json.Number)
			if !ok {
				return errors.New("Error converting go.events.channel.size to int")
			}
			chSize, err := chSizeNumber.Int64()
			if err != nil {
				return errors.New(fmt.Sprintf("Error converting go.events.channel.size to int: %s", err))
			}
			err = cfg.Librdkafka.Consumer.SetKey("go.events.channel.size", int(chSize))
			if err != nil {
				return errors.New(fmt.Sprintf("Error setting go.events.channel.size: %s", err))
			}
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

	_, rdkafkaVer := rdkafka.LibraryVersion()
	l.Printf("Spawning Consumer Manager (librdkafka %s)...", rdkafkaVer)
	var managerWg sync.WaitGroup
	managerCtx, managerCancel := context.WithCancel(ctx)
	manager := NewConsumerManager(managerCtx, cfg)

	managerWg.Add(1)
	go func() {
		defer managerWg.Done()
		manager.Run()
	}()

	var serverWg sync.WaitGroup
	serverCtx, serverCancel := context.WithCancel(ctx)
	rafka := NewServer(serverCtx, manager, 5*time.Second)

	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		err := rafka.ListenAndServe(":6380")
		if err != nil {
			log.Fatal(err)
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
