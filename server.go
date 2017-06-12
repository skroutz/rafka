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

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	redisproto "github.com/secmask/go-redisproto"

	// TODO(agis): get rid of this when we upgrade to 1.9
	"golang.org/x/sync/syncmap"
)

type Server struct {
	log      *log.Logger
	manager  *ConsumerManager
	ctx      context.Context
	inFlight sync.WaitGroup
	timeout  time.Duration

	// clientByID contains the currently connected clients to the server.
	// It's essentially a map[string]*Client
	clientByID syncmap.Map
}

func NewServer(ctx context.Context, manager *ConsumerManager, timeout time.Duration) *Server {
	return &Server{
		ctx:     ctx,
		manager: manager,
		timeout: timeout,
		log:     log.New(os.Stderr, "[server] ", log.Ldate|log.Ltime),
	}
}

func (s *Server) handleConn(conn net.Conn) {
	client := NewClient(conn, s.manager)
	s.clientByID.Store(client.id, client)
	defer s.closeClient(client)

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	var ew error
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError("ERR " + err.Error())
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
					ew = writer.WriteError("ERR " + err.Error())
					break
				}
				c, err := client.Consumer(topics)
				if err != nil {
					ew = writer.WriteError("ERR " + err.Error())
					break
				}

				// Setup timeout
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
					ew = writer.WriteError("ERR server shutdown")
				case msg := <-c.Out():
					ew = writer.WriteObjects(msgToRedis(msg)...)
				case <-ticker.C:
					ew = writer.WriteBulk(nil)
				}
			case "RPUSH":
				key := strings.ToUpper(string(command.Get(1)))
				if key != "ACKS" {
					ew = writer.WriteError("ERR You can only RPUSH to the 'acks' key")
					break
				}

				topic, partition, offset, err := parseAck(string(command.Get(2)))
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				c, err := client.ConsumerByTopic(topic)
				if err != nil {
					ew = writer.WriteError(err.Error())
					break
				}

				c.SetOffset(topic, partition, offset+1)
				ew = writer.WriteBulkString("OK")
			case "CLIENT":
				subcmd := strings.ToUpper(string(command.Get(1)))
				switch subcmd {
				case "SETNAME":
					prevID := client.id
					newID := string(command.Get(2))

					_, ok := s.clientByID.Load(newID)
					if ok {
						ew = writer.WriteError(fmt.Sprintf("id %s is already taken", newID))
						break
					}

					err := client.SetID(newID)
					if err != nil {
						ew = writer.WriteError(err.Error())
						break
					}
					s.clientByID.Store(newID, client)
					s.clientByID.Delete(prevID)
					ew = writer.WriteBulkString("OK")
				case "GETNAME":
					ew = writer.WriteBulkString(client.String())
				default:
					ew = writer.WriteError("ERR command not supported")
				}
			default:
				ew = writer.WriteError("ERR command not supported")
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

	// unblock Accept()
	go func() {
		<-s.ctx.Done()
		s.log.Printf("Shutting down...")
		listener.Close()

		// close existing clients
		closeFunc := func(id, client interface{}) bool {
			c, ok := client.(*Client)
			if !ok {
				s.log.Printf("Couldn't convert %#v to Client", c)
				return false
			}
			s.closeClient(c)
			return true
		}
		s.clientByID.Range(closeFunc)
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

	s.log.Println("Waiting for in-flight connections...")
	s.inFlight.Wait()
	s.log.Println("All connections closed. Bye!")

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

func parseAck(ack string) (string, int32, rdkafka.Offset, error) {
	parts := strings.SplitN(ack, ":", 3)
	if len(parts) != 3 {
		return "", 0, 0, fmt.Errorf("Cannot parse ack: '%s'", ack)
	}

	var err error

	partition64, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return "", 0, 0, err
	}
	partition := int32(partition64)

	offset, err := strconv.ParseInt(parts[2], 0, 64)
	if err != nil {
		return "", 0, rdkafka.Offset(0), err
	}

	return parts[0], partition, rdkafka.Offset(offset), nil
}

func msgToRedis(msg *rdkafka.Message) []interface{} {
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

// closeClient closes c's underlying connection and also signals its consumers
// to shutdown.
func (s *Server) closeClient(c *Client) {
	// We're fine with errors from Close() since we know it will happen that
	// we attempt to close an already-closed connection (eg. the client
	// closes it after we already deferred closeClient()).
	c.conn.Close()

	for cid := range c.consumers {
		c.manager.ShutdownConsumer(cid)
	}

	s.clientByID.Delete(c.id)
}
