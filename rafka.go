package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
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
	var err error
	log := log.New(os.Stderr, "[rafka] ", log.Ldate|log.Ltime)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// var consumer Consumer
	// if c.String("consumer") == "kafka" {
	// 	consumer = NewKafka(&kafkacfg)
	// } else {
	// 	consumer = NewDummy()
	// }

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 1)

	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		panic(err)
	}

	wg.Add(1)

	log.Println("Spawning Consumer Manager")
	manager := NewManager(ctx)

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		manager.Run()
	}(ctx)

	log.Println("Spawning Redis server")
	go func(ctx context.Context, manager *Manager) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Printf("[server] Shutting down...")
				return
			default:
				conn, err := listener.Accept()
				if err == nil {
					go handleConnection(manager, conn)
				} else {
					log.Println("Error on accept: ", err)
				}
			}
		}
	}(ctx, manager)

	select {
	case <-sigCh:
		log.Println("Received shutdown signal..")
		listener.Close()
		cancel()
	case <-done:
	}

	log.Println("Waiting for goroutines to finish...")
	wg.Wait()

	log.Println("Bye!")
}
