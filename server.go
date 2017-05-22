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

	"github.com/confluentinc/confluent-kafka-go/kafka"
	redisproto "github.com/secmask/go-redisproto"
	// TODO(agis): get rid of this when we upgrade to 1.9
	"golang.org/x/sync/syncmap"
)

type Server struct {
	log       *log.Logger
	manager   *Manager
	ctx       context.Context
	inFlight  sync.WaitGroup
	timeout   time.Duration
	clientIDs syncmap.Map
}

func NewServer(ctx context.Context, manager *Manager, timeout time.Duration) *Server {
	rs := Server{
		ctx:     ctx,
		manager: manager,
		timeout: timeout,
		log:     log.New(os.Stderr, "[server] ", log.Ldate|log.Ltime),
	}

	return &rs
}

func (rs *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	rafkaConn := NewConn(rs.manager)
	defer rafkaConn.Teardown(&rs.clientIDs)

	// Set a temporary ID
	rafkaConn.SetID(conn.RemoteAddr().String())

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
			case "BLPOP":
				topics, err := parseTopics(string(command.Get(1)))
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}
				c, err := rafkaConn.Consumer(topics)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				// Setup Timeout
				// Check the last argument for an int or use the default.
				// We do not support 0 as inf.
				timeout := rs.timeout
				lastIdx := command.ArgCount() - 1
				secs, err := strconv.Atoi(string(command.Get(lastIdx)))
				if err == nil {
					timeout = time.Duration(secs) * time.Second
				}
				ticker := time.NewTicker(timeout)

				select {
				case <-rs.ctx.Done():
					ew = writer.WriteError("SHUTDOWN")
				case msg := <-c.Out():
					ew = writer.WriteObjects(msgToRedis(msg)...)
				case <-ticker.C:
					// BLPOP returns nil on timeout
					ew = writer.WriteBulk(nil)
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
				c, err := rafkaConn.ConsumerByTopic(topic)
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
					id := string(command.Get(2))

					_, loaded := rs.clientIDs.LoadOrStore(id, true)
					if loaded {
						ew = writer.WriteError(fmt.Sprintf("id %s is already taken", id))
						break
					}

					err := rafkaConn.SetID(id)
					if err != nil {
						s.clientIDs.Delete(id)
						ew = writer.WriteError(err.Error())
						break
					}

					ew = writer.WriteBulkString("OK")
				case "GETNAME":
					ew = writer.WriteBulkString(rafkaConn.String())
				default:
					ew = writer.WriteError("ERR syntax error")
				}
			default:
				ew = writer.WriteError("Command not supported")
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

func (rs *Server) ListenAndServe(port string) error {
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
					rs.handleConn(conn)
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
		return nil, fmt.Errorf("Cannot parse topics: '%s'", key)
	}
	switch parts[0] {
	case "topics":
		topics := strings.Split(parts[1], ",")
		if len(topics) > 0 {
			return topics, nil
		}

		return nil, errors.New("Not enough topics")
	default:
		return nil, fmt.Errorf("Cannot parse topics: '%s'", key)
	}
}

func parseAck(ack string) (string, int32, int64, error) {
	parts := strings.SplitN(ack, ":", 3)
	if len(parts) != 3 {
		return "", 0, 0, fmt.Errorf("Cannot parse ack: '%s'", ack)
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

func msgToRedis(msg *kafka.Message) []interface{} {
	tp := msg.TopicPartition

	return []interface{}{
		[]byte("topic"),
		[]byte(*tp.Topic),
		[]byte("partition"),
		int64(tp.Partition),
		[]byte("offset"),
		int64(tp.Offset),
		[]byte("value"),
		msg.Value}
}
