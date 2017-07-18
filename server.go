package main

import (
	"bufio"
	"bytes"
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
	ctx      context.Context // TODO(agis): make this a function param
	inFlight sync.WaitGroup  // TODO(agis): make this a local var
	timeout  time.Duration

	// clientByID contains the currently connected clients to the server.
	clientByID syncmap.Map // map[string]*Client
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
	defer conn.Close()

	c := NewClient(conn, s.manager)
	defer c.Close()
	s.clientByID.Store(c.id, c)
	defer func() {
		s.clientByID.Delete(c.id)
	}()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	var writeErr error
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				writeErr = writer.WriteError(err.Error())
			} else {
				break
			}
		} else {
			cmd := strings.ToUpper(string(command.Get(0)))
			switch cmd {
			case "BLPOP": // consume
				topics, err := parseTopics(string(command.Get(1)))
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}
				cons, err := c.Consumer(topics)
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}

				// Setup timeout: Check the last argument for
				// an int or use the default.
				// Note: We do not support 0 as infinity.
				timeout := s.timeout
				lastIdx := command.ArgCount() - 1
				secs, err := strconv.Atoi(string(command.Get(lastIdx)))
				if err == nil {
					timeout = time.Duration(secs) * time.Second
				}
				ticker := time.NewTicker(timeout)

			ConsLoop:
				for {
					select {
					case <-s.ctx.Done():
						writeErr = writer.WriteError("CONS Server shutdown")
						break ConsLoop
					case <-ticker.C:
						writeErr = writer.WriteBulk(nil)
						break ConsLoop
					default:
						ev, err := cons.Poll(100)
						if err != nil {
							writeErr = writer.WriteError("CONS Poll " + err.Error())
							break ConsLoop
						}
						if ev == nil {
							continue
						}
						writeErr = writer.WriteObjects(msgToRedis(ev)...)
						break ConsLoop
					}
				}
				ticker.Stop()
			case "RPUSH": // ack (consumer)
				key := strings.ToUpper(string(command.Get(1)))
				if key != "ACKS" {
					writeErr = writer.WriteError("CONS You can only RPUSH to the 'acks' key")
					break
				}

				topic, partition, offset, err := parseAck(string(command.Get(2)))
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}

				cons, err := c.ConsumerByTopic(topic)
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}

				cons.SetOffset(topic, partition, offset+1)
				writeErr = writer.WriteBulkString("OK")
			case "RPUSHX": // produce
				argc := command.ArgCount() - 1
				if argc != 2 {
					writeErr = writer.WriteError("PROD RPUSHX accepts 2 arguments, got " + strconv.Itoa(argc))
					break
				}

				parts := bytes.Split(command.Get(1), []byte(":"))
				if len(parts) != 2 {
					writeErr = writer.WriteError("PROD First argument must be in the form of 'topics:<topic>'")
					break
				}
				topic := string(parts[1])
				tp := rdkafka.TopicPartition{Topic: &topic, Partition: rdkafka.PartitionAny}
				kafkaMsg := &rdkafka.Message{TopicPartition: tp, Value: command.Get(2)}

				prod, err := c.Producer(&cfg.Librdkafka.Producer)
				if err != nil {
					writeErr = writer.WriteError("PROD Error spawning producer: " + err.Error())
					break
				}

				err = prod.Produce(kafkaMsg)
				if err != nil {
					writeErr = writer.WriteError("PROD " + err.Error())
					break
				}
				writeErr = writer.WriteInt(1)
			case "DUMP": // flush (producer)
				if c.producer == nil {
					writeErr = writer.WriteBulkString("OK")
					break
				}

				argc := command.ArgCount() - 1
				if argc != 1 {
					writeErr = writer.WriteError("PROD DUMP accepts 1 argument, got " + strconv.Itoa(argc))
					break
				}

				timeoutMs, err := strconv.Atoi(string(command.Get(1)))
				if err != nil {
					writeErr = writer.WriteError("PROD NaN")
					break
				}
				writeErr = writer.WriteInt(int64(c.producer.Flush(timeoutMs)))
			case "CLIENT":
				subcmd := strings.ToUpper(string(command.Get(1)))
				switch subcmd {
				case "SETNAME":
					prevID := c.id
					newID := string(command.Get(2))

					_, ok := s.clientByID.Load(newID)
					if ok {
						writeErr = writer.WriteError(fmt.Sprintf("CONS id %s is already taken", newID))
						break
					}

					err := c.SetID(newID)
					if err != nil {
						writeErr = writer.WriteError("CONS " + err.Error())
						break
					}
					s.clientByID.Store(newID, c)
					s.clientByID.Delete(prevID)
					writeErr = writer.WriteBulkString("OK")
				case "GETNAME":
					writeErr = writer.WriteBulkString(c.String())
				default:
					writeErr = writer.WriteError("CONS Command not supported")
				}
			case "QUIT":
				writer.WriteBulkString("OK")
				writer.Flush()
				return
			case "PING":
				writeErr = writer.WriteBulkString("PONG")
			default:
				writeErr = writer.WriteError("Command not supported")
			}
		}
		if command.IsLast() {
			writer.Flush()
		}
		if writeErr != nil {
			break
		}
	}
}

func (s *Server) ListenAndServe(port string) error {
	// TODO(agis): maybe we want to control host through a flag
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	s.log.Print("Listening on 0.0.0.0" + port)

	go func() {
		<-s.ctx.Done() // unblock Accept()
		listener.Close()

		closeFunc := func(id, client interface{}) bool {
			c, ok := client.(*Client)
			if !ok {
				s.log.Printf("Couldn't convert %#v to Client", c)
				return false
			}
			// This ugliness is due to the go-redisproto parser's
			// not having a selectable channel for reading input.
			// We're stuck with blocking on ReadCommand() and
			// unblocking it by closing the client's connection.
			c.conn.Close()
			s.clientByID.Delete(c.id)
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

	s.log.Println("Terminating in-flight connections...")
	s.inFlight.Wait()
	s.log.Println("Bye")
	return nil
}

func parseTopics(key string) ([]string, error) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("Cannot parse topics from `%s`", key)
	}
	switch parts[0] {
	case "topics":
		topics := strings.Split(parts[1], ",")
		if len(topics) > 0 {
			return topics, nil
		}
		return nil, errors.New(fmt.Sprintf("Not enough topics in `%s`", key))
	default:
		return nil, fmt.Errorf("Cannot parse topics from `%s`", key)
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
