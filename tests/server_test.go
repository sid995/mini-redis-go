package tests

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/sid995/mini-redis/internal/resp"
	"github.com/sid995/mini-redis/internal/server"
)

func TestServerIntegration(t *testing.T) {
	// Start server on a random port
	config := &server.Config{
		Address:        ":0", // Random port
		RequestTimeout: 5 * time.Second,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
	}

	srv, err := server.NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.Stop()

	// Start server in background
	go srv.Start()
	time.Sleep(100 * time.Millisecond) // Give server time to start

	// Connect to server
	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Test PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	reader := bufio.NewReader(conn)
	val, err := resp.Parse(reader)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if val.Str != "PONG" {
		t.Errorf("Expected PONG, got %s", val.Str)
	}
}
