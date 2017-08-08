// Rafka: Kafka exposed with a Redis API
//
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

var (
	cfg      Config
	shutdown = make(chan os.Signal, 1)
)

func main() {
	app := cli.NewApp()
	app.Name = "rafka"
	app.Usage = "Kafka with a Redis API"
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host",
			Usage: "Host to listen to",
			Value: "0.0.0.0",
		},
		cli.IntFlag{
			Name:  "port, p",
			Usage: "Port to listen to",
			Value: 6380,
		},
		cli.StringFlag{
			Name:  "kafka, k",
			Usage: "Load librdkafka configuration from `FILE`",
			Value: "kafka.json",
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

		// cfg might be set before main() runs (eg. while testing)
		if cfg.Host == "" {
			cfg.Host = c.String("host")
		}
		if cfg.Port == 0 {
			cfg.Port = c.Int("port")
		}

		// republish config using rdkafka.SetKey() for proper error checking
		for _, config := range []rdkafka.ConfigMap{cfg.Librdkafka.Consumer, cfg.Librdkafka.Producer} {
			// merge general configuration
			for k, v := range cfg.Librdkafka.General {
				if config[k] != nil {
					continue
				}
				err = config.SetKey(k, v)
				if err != nil {
					return fmt.Errorf("Error in librdkafka config (%s): %s", k, err)
				}
			}

			for k, v := range config {
				err = config.SetKey(k, v)
				if err != nil {
					return fmt.Errorf("Error in librdkafka config (%s): %s", k, err)
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
				return fmt.Errorf("Error converting go.events.channel.size to int: %s", err)
			}
			err = cfg.Librdkafka.Consumer.SetKey("go.events.channel.size", int(chSize))
			if err != nil {
				return fmt.Errorf("Error setting go.events.channel.size: %s", err)
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

	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	ctx := context.Background()

	_, rdkafkaVer := rdkafka.LibraryVersion()
	l.Printf("Spawning Consumer Manager (librdkafka %s) | config: %v...", rdkafkaVer, cfg)
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
		err := rafka.ListenAndServe(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
		if err != nil {
			log.Fatal(err)
		}

	}()

	<-shutdown
	l.Println("Received shutdown signal. Shutting down...")
	serverCancel()
	serverWg.Wait()
	managerCancel()
	managerWg.Wait()
	l.Println("All components shut down. Bye!")
}
