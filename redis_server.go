package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	redisproto "github.com/secmask/go-redisproto"
)

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
	defer rafka_con.Teardown()

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
				c, err := rafka_con.Consumer(topics)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				ticker := time.NewTicker(rs.timeout)
				select {
				case <-rs.ctx.Done():
					ew = writer.WriteError("SHUTDOWN")
				case msg := <-c.Out():
					ew = writer.WriteBulkString(msg)
				case <-ticker.C:
					ew = writer.WriteError("TIMEOUT")
				}
			case "RPUSH":
				// Only allow rpush commits <ack>
				key := strings.ToUpper(string(command.Get(1)))
				if key != "ACKS" {
					ew = writer.WriteError("ERR You can only push to the 'acks' key")
					break
				}

				// Parse Ack
				topic, partition, offset, err := parseAck(string(command.Get(2)))
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}
				// Get Consumer
				c, err := rafka_con.ConsumerByTopic(topic)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				// Ack
				// TODO blocking?
				err = c.Ack(topic, partition, offset)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				} else {
					ew = writer.WriteBulkString("OK")
				}
			case "DEL":
				id := (ConsumerID)(command.Get(1))
				deleted := rs.manager.Delete(id)
				if deleted {
					ew = writer.WriteInt(1)
				} else {
					ew = writer.WriteInt(0)
				}
			case "CLIENT":
				subcmd := strings.ToUpper(string(command.Get(1)))
				switch subcmd {
				case "SETNAME":
					rafka_con.SetID(string(command.Get(2)))
					ew = writer.WriteBulkString("OK")
				case "GETNAME":
					ew = writer.WriteBulkString(rafka_con.String())
				default:
					ew = writer.WriteError("ERR syntax error")
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

func parseAck(ack string) (string, int32, int64, error) {
	parts := strings.SplitN(ack, ":", 3)
	if len(parts) != 3 {
		return "", 0, 0, errors.New(fmt.Sprintf("Cannot parse ack: '%s'", ack))
	}
	var err error

	// Partition
	partition64, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return "", 0, 0, err
	}
	partition := int32(partition64)

	// Offset
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", 0, 0, err
	}

	return parts[0], partition, offset, nil
}
