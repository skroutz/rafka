// Copyright (C) 2017 Skroutz S.A.
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
//
// +build go1.9

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
)

type Server struct {
	log     *log.Logger
	manager *ConsumerManager

	// default timeout for consumer poll
	timeout time.Duration

	// currently connected clients
	clientByID sync.Map // map[string]*Client
}

func NewServer(manager *ConsumerManager, timeout time.Duration) *Server {
	return &Server{
		manager: manager,
		timeout: timeout,
		log:     log.New(os.Stderr, "[server] ", log.Ldate|log.Ltime),
	}
}

func (s *Server) Handle(ctx context.Context, conn net.Conn) {
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
			// Consume the next message from one or more topics
			//
			// BLPOP topics:<topic>:<JSON-encoded consumer config> <timeoutMs>
			case "BLPOP":
				arg1 := string(command.Get(1))
				topics, cfg, err := parseTopicsAndConfig(arg1)
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}
				cons, err := c.Consumer(topics, cfg)
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
					case <-ctx.Done():
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
			// Get producer/consumer statistics
			//
			// HGETALL stats
			case "HGETALL":
				key := strings.ToUpper(string(command.Get(1)))
				if key != "STATS" {
					writeErr = writer.WriteError("ERR Expected key to be 'stats', got " + key)
					break
				}
				writeErr = writer.WriteObjects(stats.toRedis()...)
			// List all topics
			//
			// KEYS
			case "KEYS":
				arg1 := string(command.Get(1))
				if arg1 != "topics:" {
					writeErr = writer.WriteError("ERR Expected argument to be 'topics:', got " + arg1)
					break
				}

				prod, err := c.Producer(cfg.Librdkafka.Producer)
				if err != nil {
					writeErr = writer.WriteError("ERR Error spawning producer: " + err.Error())
					break
				}

				metadata, err := prod.rdProd.GetMetadata(nil, true, 100)
				if err != nil {
					writeErr = writer.WriteError("ERR Error getting metadata: " + err.Error())
					break
				}

				var topic_names []interface{}

				for topic_name, _ := range metadata.Topics {
					topic_names = append(topic_names, "topics:"+topic_name)
				}

				if len(topic_names) > 0 {
					writeErr = writer.WriteObjects(topic_names...)
				} else {
					// we need to return empty array here
					_, writeErr = writer.Write([]byte{'*', '0', '\r', '\n'})
				}
			// Reset producer/consumer statistics
			//
			// DEL stats
			case "DEL":
				key := strings.ToUpper(string(command.Get(1)))
				if key != "STATS" {
					writeErr = writer.WriteError("ERR Expected key to be 'stats', got " + key)
					break
				}
				stats.Reset()
				writeErr = writer.WriteInt(1)
			// Commit offsets for the given topic/partition
			//
			// RPUSH acks <topic>:<partition>:<offset>
			case "RPUSH":
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

				err = cons.SetOffset(topic, partition, offset+1)
				if err != nil {
					writeErr = writer.WriteError("CONS " + err.Error())
					break
				}

				writeErr = writer.WriteInt(1)
			// Produce a message
			//
			// RPUSHX topics:<topic> <message>
			case "RPUSHX":
				argc := command.ArgCount() - 1
				if argc != 2 {
					writeErr = writer.WriteError("PROD RPUSHX accepts 2 arguments, got " + strconv.Itoa(argc))
					break
				}

				parts := bytes.Split(command.Get(1), []byte(":"))
				if len(parts) != 2 && len(parts) != 3 {
					errMsg := "PROD First argument must be in the form of 'topics:<topic>' or 'topics:<topic>:<key>'"
					writeErr = writer.WriteError(errMsg)
					break
				}
				topic := string(parts[1])
				tp := rdkafka.TopicPartition{Topic: &topic, Partition: rdkafka.PartitionAny}
				kafkaMsg := &rdkafka.Message{TopicPartition: tp, Value: command.Get(2)}

				if len(parts) == 3 {
					kafkaMsg.Key = parts[2]
				}

				prod, err := c.Producer(cfg.Librdkafka.Producer)
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
			// Flush the producer
			//
			// DUMP <timeoutMs>
			case "DUMP":
				if c.producer == nil {
					writeErr = writer.WriteInt(0)
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
				// Set the consumer group.id
				//
				// CLIENT SETNAME <group.id>:<name>
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
			// TODO(agis) log these errors
			break
		}
	}
}

func (s *Server) ListenAndServe(ctx context.Context, hostport string) error {
	var inflightWg sync.WaitGroup

	listener, err := net.Listen("tcp", hostport)
	if err != nil {
		return err
	}
	s.log.Print("Listening on " + hostport)

	go func() {
		<-ctx.Done() // unblock Accept()
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
		case <-ctx.Done():
			break Loop
		default:
			conn, err := listener.Accept()
			if err != nil {
				// we know that closing a listener that blocks
				// on Accept() will return this error
				if !strings.Contains(err.Error(), "use of closed network connection") {
					s.log.Println("Accept error: ", err)
				}
			} else {
				inflightWg.Add(1)
				go func() {
					defer inflightWg.Done()
					s.Handle(ctx, conn)
				}()
			}
		}
	}

	s.log.Println("Waiting for in-flight connections...")
	inflightWg.Wait()
	s.log.Println("Bye")
	return nil
}

// parseTopicsAndConfig parses the "topics:topic1,topic2:{config}" into
// an array of topics and a config map
func parseTopicsAndConfig(s string) ([]string, rdkafka.ConfigMap, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 2 || parts[0] != "topics" {
		return nil, nil, fmt.Errorf("Cannot parse topics from `%s`", s)
	}

	topics := strings.Split(parts[1], ",")
	if len(topics) == 0 {
		return nil, nil, fmt.Errorf("Not enough topics in `%s`", s)
	}

	var rdconfig rdkafka.ConfigMap
	if len(parts) == 3 && parts[2] != "" {
		dec := json.NewDecoder(strings.NewReader(parts[2]))
		dec.UseNumber()
		err := dec.Decode(&rdconfig)
		if err != nil {
			return nil, nil, fmt.Errorf("Error parsing configuration from '%s'; %s", parts[2], err)
		}
	}

	return topics, rdconfig, nil
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
		"topic",
		*tp.Topic,
		"partition",
		int64(tp.Partition),
		"offset",
		int64(tp.Offset),
		"value",
		msg.Value}
}
