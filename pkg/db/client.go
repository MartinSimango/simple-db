package db

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	ErrTimeout = fmt.Errorf("request timed out")
)

type SimpleDbClient struct {
	c    net.Conn
	addr string
	mu   sync.Mutex
}

func NewSimpleDbClient(address string) (*SimpleDbClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &SimpleDbClient{
		c:    conn,
		addr: address,
	}, nil
}

func (client *SimpleDbClient) Close() error {
	return client.c.Close()
}

func (client *SimpleDbClient) Reconnect() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.reconnect()
}

func (client *SimpleDbClient) reconnect() error {
	if err := client.c.Close(); err != nil {
		return err
	}
	conn, err := net.Dial("tcp", client.addr)
	if err != nil {
		return err
	}
	client.c = conn
	return nil
}

func (client *SimpleDbClient) Put(ctx context.Context, key, value string) (string, error) {
	return client.queryServer(ctx, request{
		operation: PUT,
		key:       key,
		headers: map[string]string{
			LENGTH_HEADER: fmt.Sprintf("%d", len(value)),
		},
		body: &value,
	})
}

func (client *SimpleDbClient) Get(ctx context.Context, key string) (string, error) {
	return client.queryServer(ctx, request{
		operation: GET,
		key:       key,
	})
}

func (client *SimpleDbClient) Delete(ctx context.Context, key string) (string, error) {
	return client.queryServer(ctx, request{
		operation: DELETE,
		key:       key,
	})
}

type request struct {
	operation Operation
	key       string
	headers   map[string]string
	body      *string
}

func (r request) Marshal() []byte {
	req := fmt.Sprintf("%s\nKey: %s\n", r.operation, r.key)
	for k, v := range r.headers {
		req += fmt.Sprintf("%s: %s\n", k, v)
	}
	req += "\n" // leave a blank line between headers and body
	if r.body != nil {
		req += *r.body
	}
	return []byte(req)
}

func (client *SimpleDbClient) queryServer(ctx context.Context, r request) (string, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	_, err := client.c.Write(r.Marshal())
	if err != nil {
		return "", fmt.Errorf("failed to send request to server: %s", err)
	}
	buf := make([]byte, 1024)

	t, _ := ctx.Deadline()
	client.c.SetReadDeadline(t)
	n, err := client.c.Read(buf)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return "", ErrTimeout
		}
		return "", fmt.Errorf("failed to read server response: %s", err)
	}

	m := string(buf[:n])
	if strings.HasPrefix(m, "ERROR: ") {
		client.reconnect() // server ends connection on error, so reconnect
		return "", fmt.Errorf("failed to process request: %s", m)
	}
	return m, nil

}
