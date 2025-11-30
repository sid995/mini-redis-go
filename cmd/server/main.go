package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sid995/mini-redis/internal/server"
)

func main() {
	// create server with default configuration
	config := server.DefaultConfig()
	srv, err := server.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// wait for interrupt signal
	<-sigChan
	log.Println("Shutting down server...")

	// Stop server
	if err := srv.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}
	log.Println("Server shutdown complete")
}
