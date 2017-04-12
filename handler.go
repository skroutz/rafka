package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	redisproto "github.com/secmask/go-redisproto"
	"golang.skroutz.gr/skroutz/rafka/kafka"
)

type clientID string

type RedisConnection struct {
	id      clientID
	manager *Manager
	log     *log.Logger
}

func NewRedisConnection(manager *Manager) *RedisConnection {
	rc := RedisConnection{
		manager: manager,
		log:     log.New(os.Stderr, "[redis-connection] ", log.Ldate|log.Ltime),
	}

	return &rc
}

func (rc *RedisConnection) SetID(id string) {
	rc.id = clientID(id)
}

func (rc *RedisConnection) String() string {
	return string(rc.id)
}

func (rc *RedisConnection) Consumer(topics []string) *kafka.Consumer {
	// TODO check if id is set
	consumerID := ConsumerID(fmt.Sprintf("%s|%s", rc.id, topics))
	return rc.manager.Get(consumerID, topics)
}

func (rc *RedisConnection) Close() {
	rc.log.Printf("[%s] Closing()", rc.id)
}

type RedisServer struct {
	log      *log.Logger
	manager  *Manager
	ctx      context.Context
	inFlight sync.WaitGroup
	timeout  time.Duration
}

func NewRedisServer(ctx context.Context, manager *Manager, timeout time.Duration) *RedisServer {
	rs := RedisServer{
		ctx:     ctx,
		manager: manager,
		timeout: timeout,
		log:     log.New(os.Stderr, "[redis] ", log.Ldate|log.Ltime),
	}

	return &rs
}

func (rs *RedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	rafka_con := NewRedisConnection(rs.manager)
	defer rafka_con.Close()

	// Set a temporary ID
	rafka_con.SetID(conn.RemoteAddr().String())

	var ew error
	for {
		// TODO: is this blocking?
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				rs.log.Println(err, ", closed connection to", conn.RemoteAddr())
				break
			}
		} else {
			cmd := strings.ToUpper(string(command.Get(0)))
			switch cmd {
			case "PING":
				ew = writer.WriteBulkString("PONG")
			case "GET":
				topics, err := parseTopics(string(command.Get(1)))
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}
				c := rafka_con.Consumer(topics)
				ticker := time.NewTicker(rs.timeout)
				select {
				case <-rs.ctx.Done():
					ew = writer.WriteError("SHUTDOWN")
				case msg := <-c.Out():
					ew = writer.WriteBulkString(msg)
				case <-ticker.C:
					ew = writer.WriteError("TIMEOUT")
				}
			case "DEL":
				id := (ConsumerID)(command.Get(1))
				deleted := rs.manager.Delete(id)
				if deleted {
					ew = writer.WriteInt(1)
				} else {
					ew = writer.WriteInt(0)
				}
			default:
				ew = writer.WriteError("Command not support")
			}
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			rs.log.Println("Connection closed", ew)
			break
		}
	}
}

func (rs *RedisServer) ListenAndServe(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	// Unblock Accept()
	go func() {
		<-rs.ctx.Done()
		rs.log.Printf("Shutting down...")
		listener.Close()
	}()

Loop:
	for {
		select {
		case <-rs.ctx.Done():
			break Loop
		default:
			conn, err := listener.Accept()
			if err == nil {
				rs.inFlight.Add(1)

				go func() {
					defer rs.inFlight.Done()
					rs.handleConnection(conn)
				}()
			} else {
				rs.log.Println("Error on accept: ", err)
			}
		}
	}

	rs.log.Println("Waiting for inflight connections...")
	rs.inFlight.Wait()
	rs.log.Println("All connections handled, Bye!")

	return nil
}

func parseTopics(key string) ([]string, error) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New(fmt.Sprintf("Cannot parse topics: '%s'", key))
	}
	switch parts[0] {
	case "topics":
		topics := strings.Split(parts[1], ",")
		if len(topics) > 0 {
			return topics, nil
		} else {
			return nil, errors.New("Not enough topics")
		}

	default:
		return nil, errors.New(fmt.Sprintf("Cannot parse topics: '%s'", key))
	}
}
