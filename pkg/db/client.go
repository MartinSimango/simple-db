package db

import (
	"context"
	"fmt"
	"net"
	"sync"
)

type SimpleDbClient struct {
	c  net.Conn
	mu sync.Mutex
}

func NewSimpleDbClient(address string) (*SimpleDbClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &SimpleDbClient{
		c: conn,
	}, nil
}

func (client *SimpleDbClient) Close() error {
	return client.c.Close()
}

func (client *SimpleDbClient) Reconnect(address string) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.c.Close(); err != nil {
		return err
	}
	conn, err := net.Dial("tcp", address)
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
	in := make(chan int, 1)
	e := make(chan error, 1)

	go func() {
		n, err := client.c.Read(buf)
		if err != nil {
			e <- fmt.Errorf("failed to read server response: %s", err)
		}
		in <- n

	}()

	select {
	case err := <-e:
		return "", err
	case n := <-in:
		return string(buf[:n]), nil
	case <-ctx.Done():
		return "", fmt.Errorf("request timed out: %s", ctx.Err())
	}

}
