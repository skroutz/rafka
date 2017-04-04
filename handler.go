package main

import (
	"bufio"
	"context"
	"log"
	"net"
	"os"
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
				id := (ConsumerID)(command.Get(1))
				c := rs.manager.Get(id)
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
