package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
)

func main() {

	// start tcp server
	// ctx := context.Background
	l, err := net.Listen("tcp", ":5050")
	if err != nil {
		panic(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			slog.Debug("Handling client: ", "Addr", c.RemoteAddr())
			data := make([]byte, 1024, 1024) // 1KB buffer
			n, err := conn.Read(data)
			if err != nil {
				slog.Error("Error reading from connection: ", "error", err)
				return
			}
			slog.Info("Received payload", "Addr", c.RemoteAddr(), "payload", string(data[:n]), "bytesRead", n)

			scanner := bufio.NewScanner(bytes.NewReader(data[:n]))

			buf := make([]byte, 0, 1024) // 1KB buffer
			scanner.Buffer(buf, 1024)
			if !scanner.Scan() {
				slog.Warn("No data received")
				return
			}
			operation := scanner.Text()
			if !scanner.Scan() {
				slog.Warn("No data received after operation", "operation", operation)
				return
			}
			keyHeader := scanner.Text()
			key := strings.Split(keyHeader, "KEY: ")[1]

			if !scanner.Scan() {
				slog.Warn("No data received after key", "key", key)
				return
			}
			lengthHeader := scanner.Text()
			length := strings.Split(lengthHeader, "LENGTH: ")[1]

			payloadLen, err := strconv.Atoi(length)
			if err != nil {
				slog.Warn("Invalid length", "length", length)
				return
			}

			bytesRead := len(operation) + 1 + len(keyHeader) + 1 + len(lengthHeader) + 1
			payload := string(data[bytesRead : bytesRead+payloadLen])
			slog.Info("Payload extracted", "payload", payload)

			var operationErr error
			switch operation {
			case PUT:
				slog.Debug("Processing PUT")
				operationErr = processPut(key, payload)
			case GET:
				slog.Debug("Processing GET")
			case DELETE:
				slog.Debug("Processing DELETE")
			default:
				slog.Warn("Unknown operation", "operation", operation)
			}

			if operationErr != nil {
				conn.Write([]byte("ERROR: " + operationErr.Error()))
			} else {
				conn.Write([]byte("OK"))
			}

		}(conn)
	}
}

type PutCommand struct {
	Key   string
	Value string
}

func processPut(key string, value string) error {
	slog.Info("Putting key/value", "key", key, "value", value)
	return nil
}
