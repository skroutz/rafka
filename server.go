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
	manager   *ConsumerManager
	ctx       context.Context
	inFlight  sync.WaitGroup
	timeout   time.Duration
	clientIDs syncmap.Map // map[string]*Client
}

func NewServer(ctx context.Context, manager *ConsumerManager, timeout time.Duration) *Server {
	s := Server{
		ctx:     ctx,
		manager: manager,
		timeout: timeout,
		log:     log.New(os.Stderr, "[server] ", log.Ldate|log.Ltime),
	}

	return &s
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	client := NewClient(s.manager)
	defer client.Teardown(&s.clientIDs)

	// Set a temporary ID
	client.SetID(conn.RemoteAddr().String())

	var ew error
	for {
		// TODO: is this blocking?
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				s.log.Println(err, "closed connection to", client.id)
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
				c, err := client.Consumer(topics)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				// Setup Timeout
				// Check the last argument for an int or use the default.
				// We do not support 0 as inf.
				timeout := s.timeout
				lastIdx := command.ArgCount() - 1
				secs, err := strconv.Atoi(string(command.Get(lastIdx)))
				if err == nil {
					timeout = time.Duration(secs) * time.Second
				}
				ticker := time.NewTicker(timeout)

				select {
				case <-s.ctx.Done():
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
				c, err := client.ConsumerByTopic(topic)
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
				deleted := s.manager.ShutdownConsumer(id)
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

					_, loaded := s.clientIDs.LoadOrStore(id, true)
					if loaded {
						ew = writer.WriteError(fmt.Sprintf("id %s is already taken", id))
						break
					}

					err := client.SetID(id)
					if err != nil {
						s.clientIDs.Delete(id)
						ew = writer.WriteError(err.Error())
						break
					}

					ew = writer.WriteBulkString("OK")
				case "GETNAME":
					ew = writer.WriteBulkString(client.String())
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
			s.log.Println("Connection closed", ew)
			break
		}
	}
}

func (s *Server) ListenAndServe(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	// Unblock Accept()
	go func() {
		<-s.ctx.Done()
		s.log.Printf("Shutting down...")
		listener.Close()
	}()

Loop:
	for {
		select {
		case <-s.ctx.Done():
			break Loop
		default:
			conn, err := listener.Accept()
			if err != nil {
				// we know that closing a listener that blocks
				// on accepts will return this error
				if !strings.Contains(err.Error(), "use of closed network connection") {
					s.log.Println("Accept error: ", err)
				}
			} else {
				s.inFlight.Add(1)
				go func() {
					defer s.inFlight.Done()
					s.handleConn(conn)
				}()
			}
		}
	}

	s.log.Println("Waiting for inflight connections...")
	s.inFlight.Wait()
	s.log.Println("All connections handled, Bye!")

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
