package main

import (
	"bufio"
	"log"
	"net"
	"strings"

	redisproto "github.com/secmask/go-redisproto"
)

func handleConnection(manager *Manager, conn net.Conn) {
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
				log.Println(err, " closed connection to ", conn.RemoteAddr())
				break
			}
		} else {
			cmd := strings.ToUpper(string(command.Get(0)))
			switch cmd {
			case "PING":
				ew = writer.WriteBulkString("PONG")
			case "GET":
				id := (ConsumerID)(command.Get(1))
				c := manager.Get(id)
				log.Printf("%#v", c)
				msg := <-c.Out()
				ew = writer.WriteBulkString(msg)
			case "DEL":
				ew = writer.WriteBulkString("OK")
			default:
				ew = writer.WriteError("Command not support")
			}
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			log.Println("Connection closed", ew)
			break
		}
	}
}
