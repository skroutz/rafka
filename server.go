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

type monitorReply struct {
	timestamp     int64
	monitorString string
}

type Server struct {
	log      *log.Logger
	listener net.Listener

	// default timeout for consumer poll
	timeout time.Duration

	// currently connected clients
	clientByID sync.Map // map[string]*Client

	// server's main monitor channel
	srvMonitorChan chan monitorReply

	// monitor channels of connected monitoring clients
	monitorChans sync.Map // map[string](chan string)
}

func NewServer(timeout time.Duration) *Server {
	// The default is 64kB, however Kafka messages might typically be
	// much larger than this, so we bump the limit to 32MB.
	//
	// Keep in mind that the Redis protocol specifies strings may be
	// up to 512MB, although there are plans to make them even bigger
	// (see https://github.com/antirez/redis/issues/757).
	redisproto.MaxBulkSize = 32 * 1024 * 1000

	return &Server{
		timeout:        timeout,
		log:            log.New(os.Stderr, "[server] ", log.Ldate|log.Ltime),
		srvMonitorChan: make(chan monitorReply, 1000),
	}
}

// formatMonitorCommand simply wraps the Redis command and its arguments into double quotes.
func formatMonitorCommand(cmd *redisproto.Command) string {
	commandStr := fmt.Sprintf("\"%s\"", string(cmd.Get(0)))
	for i := 1; i < cmd.ArgCount(); i++ {
		commandStr += fmt.Sprintf(" \"%s\"", string(cmd.Get(i)))
	}
	return commandStr
}

// monitorHandler outputs every command processed by Rafka server to the monitor channels of the
// connected monitoring clients.
func (s *Server) monitorHandler() {
	for monReply := range s.srvMonitorChan {
		s.monitorChans.Range(func(id, channel interface{}) bool {
			clientMonitorChan, ok := channel.(chan string)
			if !ok {
				s.log.Printf("Couldn't cast %#v to chan string", channel)
				return false
			}

			select {
			case clientMonitorChan <- fmt.Sprintf("%.6f [0 %s] %s",
				float64(monReply.timestamp)/1e+9,
				id,
				monReply.monitorString):
			default:
				s.log.Printf("Failed to write monitor string to client '%s'\n", id)
			}

			return true
		})
	}
}

func (s *Server) Handle(ctx context.Context, conn net.Conn) {
	c := NewClient(ctx, conn, cfg)
	defer c.Close()

	s.clientByID.Store(c.id, c)
	defer func() {
		s.clientByID.Delete(c.id)
		s.monitorChans.Delete(c.id)
	}()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	var parseErr, writeErr error
	var monitorStrCommand string

	for {
		var command *redisproto.Command
		command, parseErr = parser.ReadCommand()
		if parseErr != nil {
			writeErr = writer.WriteError(parseErr.Error())
		} else {
			monitorStrCommand = formatMonitorCommand(command)
			select {
			case s.srvMonitorChan <- monitorReply{
				timestamp:     time.Now().UnixNano(),
				monitorString: monitorStrCommand,
			}:
			default:
				s.log.Printf("Could not write redis Command to channel: '%s'\n", monitorStrCommand)
			}

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
						// WriteBulkStrings() is the only method
						// that returns what the Redis Protocol
						// calls a "null array" (i.e. "*-1\r\n")
						//
						// see https://github.com/secmask/go-redisproto/issues/4
						writeErr = writer.WriteBulkStrings(nil)
						break ConsLoop
					default:
						// we set a small timeout since
						// Poll holds a lock that
						// prevents the consumer to
						// terminate until Poll returns
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

				if c.consumer == nil {
					writeErr = writer.WriteError("CONS No consumer registered for Client " + c.id)
					break
				}

				err = c.consumer.SetOffset(topic, partition, offset+1)
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
				// Set the consumer group.id and name
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
				// Get the consumer group.id and name
				case "GETNAME":
					writeErr = writer.WriteBulkString(c.id)
				default:
					writeErr = writer.WriteError("Command not supported: " + subcmd)
				}
			case "QUIT":
				writer.WriteBulkString("OK")
				writer.Flush()
				return
			case "PING":
				writeErr = writer.WriteSimpleString("PONG")
			// Stream back every command processed by the Rafka server.
			//
			// MONITOR
			case "MONITOR":
				argCnt := command.ArgCount()
				if argCnt > 1 {
					writeErr = writer.WriteError("ERR command 'monitor' does not accept any extra arguments")
					break
				}
				s.monitorChans.Store(c.id, c.monitorChan)
				s.log.Printf("New monitor client: %s (active monitors: %d)\n", c.id, s.activeMonitors())
				writer.WriteSimpleString("OK")
			default:
				writeErr = writer.WriteError("Command not supported: " + cmd)
			}
		}
		if parseErr != nil || command.IsLast() {
			writer.Flush()
		}
		if parseErr != nil || writeErr != nil {
			// parse errors are returned to the client and write
			// errors are non-issues, since they just indicate
			// the client closed the connection. That's why
			// we don't log anything.
			//
			// Instead, we close the connection. Clients should
			// establish the connections anew if needed.
			break
		}
	}
}

func (s *Server) ListenAndServe(ctx context.Context, hostport string) error {
	var wg, inflightWg sync.WaitGroup
	var err error

	s.listener, err = net.Listen("tcp", hostport)
	if err != nil {
		return err
	}
	s.log.Print("Listening on " + hostport)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.monitorHandler()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		s.shutdown()
	}()

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			conn, err := s.listener.Accept()
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

	wg.Wait()
	inflightWg.Wait()
	return nil
}

// shutdown closes current clients and also stops accepting new clients in a
// non-blocking manner
func (s *Server) shutdown() {
	// Close clients with at least 1 consumer. These clients may have
	// producers too, but we assume that they are non-critical so we close
	// them at this point too, which is earlier than the others (see below).
	s.clientByID.Range(func(id, client interface{}) bool {
		c, ok := client.(*Client)
		if !ok {
			s.log.Printf("Couldn't convert %#v to Client", c)
			return false
		}

		if c.consumer != nil {
			// This ugliness is due to the go-redisproto parser's
			// not having a selectable channel for reading input.
			// We're stuck with blocking on ReadCommand() and
			// unblocking it by closing the client's connection.
			c.conn.Close()
		}

		return true
	})

	// stop accepting new clients and unblock Accept()
	err := s.listener.Close()
	if err != nil {
		log.Printf("error closing listener: %s", err)
	}

	// close the rest of the clients (ie. those that have only producers).
	s.clientByID.Range(func(id, client interface{}) bool {
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
		return true
	})

	// ensure that the server's monitoring channel is closed
	close(s.srvMonitorChan)
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

// activeMonitors returns the connected monitoring client count.
func (s *Server) activeMonitors() int {
	monitorCnt := 0

	s.monitorChans.Range(func(id, channel interface{}) bool {
		_, ok := channel.(chan string)
		if !ok {
			s.log.Printf("Couldn't cast %#v to chan string", channel)
			return false
		}

		monitorCnt += 1
		return true
	})

	return monitorCnt
}
