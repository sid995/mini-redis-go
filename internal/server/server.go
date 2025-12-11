package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/sid995/mini-redis/internal/aof"
	"github.com/sid995/mini-redis/internal/commands"
	"github.com/sid995/mini-redis/internal/resp"
	"github.com/sid995/mini-redis/internal/store"
)

type Server struct {
	store          *store.Store
	handler        *commands.Handler
	listener       net.Listener
	persistence    *aof.PersistenceManager
	requestTimeout time.Duration // requestTimeout is the timeout for each request
	readTimeout    time.Duration // readTimeout is the timeout for reading from the connection
	writeTimeout   time.Duration // writeTimeout is the timeout for writing to the connection
}

// Config holds configuration for the server
type Config struct {
	Address        string        // Address is the address to listen on
	RequestTimeout time.Duration // RequestTimeout is the ttimeout for processing each request
	ReadTimeout    time.Duration // ReadTimeout is the timeout for reading from connections
	WriteTimeout   time.Duration // WriteTimeout is the timeout for writing to connections
	// PersistenceConfig is the configuration for persistence
	PersistenceConfig *aof.PersistenceConfig
}

// DefaultConfig returns a default server configuration
func DefaultConfig() *Config {
	return &Config{
		Address:        ":6379",
		RequestTimeout: 30 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

// NewServer creates a new server instance with the given confuration
func NewServer(config *Config) (*Server, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Create the store
	s := store.NewStore()

	// create the command handler
	handler := commands.NewHandler(s)

	// Create persistence manager
	var persistence *aof.PersistenceManager
	if config.PersistenceConfig != nil {
		var err error
		persistence, err = aof.NewPersistenceManager(s, handler, config.PersistenceConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create persistence manager: %w", err)
		}

		// Recover from snapshot andd AOF
		if err := persistence.Recover(); err != nil {
			log.Printf("Warning: recovery failed: %v", err)
		}

		// Set AOF writer in handler
		if persistence.GetAOFWriter() != nil {
			handler.SetAOF(persistence.GetAOFWriter())
		}
	}

	// create the listener
	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Address, err)
	}

	return &Server{
		store:          s,
		handler:        handler,
		listener:       listener,
		requestTimeout: config.RequestTimeout,
		readTimeout:    config.ReadTimeout,
		writeTimeout:   config.WriteTimeout,
	}, nil
}

// Start starts the server and begins accepting connections
// It blocks until ther server is stopped
func (s *Server) Start() error {
	log.Printf("Server starting on %s", s.listener.Addr())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// check if the listener was closed
			if errors.Is(err, net.ErrClosed) {
				// Listener was closed, exit the loop
				return nil
			}
			// Log other erros and continue
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		// Spawn a go routine to handle the connection
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	log.Println("Server is shutting down...")

	// Close the listener to stop accepting new connections
	if err := s.listener.Close(); err != nil {
		return err
	}

	// Write final snapshot before closing
	if s.persistence != nil {
		if err := s.persistence.WriteSnapshot(); err != nil {
			log.Printf("Warning: failed to write final snapshot: %v", err)
		}
		if err := s.persistence.Close(); err != nil {
			log.Printf("Warning: failed to close persistence manager: %v", err)
		}
	}

	// Close the store (Stops the janitor goroutine)
	s.store.Close()

	return nil
}

// Addr returns the network address the server is listening on
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// handleConnection handles a single client connection
// It processes commands until the connection is closed or an error occurs
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("Client connected: %s", clientAddr)

	reader := bufio.NewReader(conn)

	for {
		// Set read deadlione for this request
		if err := conn.SetReadDeadline(time.Now().Add(s.readTimeout)); err != nil {
			log.Printf("Error setting read deadline for %s: %v", clientAddr, err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.requestTimeout)

		// Parse the RESP command
		val, err := resp.Parse(reader)
		if err != nil {
			cancel()
			if err == io.EOF {
				log.Printf("Client disconnected: %s", clientAddr)
				return
			}
			log.Printf("Error parsing command from %s: %v", clientAddr, err)
			// Send error response
			s.sendError(conn, fmt.Sprintf("ERR %v", err))
			return
		}

		// Extract command and arguments
		command, args, err := resp.ToCommand(val)
		if err != nil {
			cancel()
			log.Printf("Error extracting command from %s: %v", clientAddr, err)
			s.sendError(conn, fmt.Sprintf("ERR %v", err))
			continue
		}

		// Handle QUIT command specially (close connection)
		if command == "QUIT" {
			cancel()
			s.handler.Execute(command, args)
			log.Printf("Client disconnected: %s", clientAddr)
			return
		}

		// Execute the command with timeout
		response := make(chan []byte, 1)
		go func() {
			response <- s.handler.Execute(command, args)
		}()

		// wait for response or timeout
		select {
		case respBytes := <-response:
			cancel()
			// Set write deadline
			if err := conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
				log.Printf("Error setting write deadline for %s: %v", clientAddr, err)
				return
			}

			// Send response
			if _, err := conn.Write(respBytes); err != nil {
				log.Printf("Error writing response to %s: %v", clientAddr, err)
				return
			}
		case <-ctx.Done():
			cancel()
			log.Printf("Request timed out for %s", clientAddr)
			s.sendError(conn, "ERR request timeout")
			return
		}
	}
}

// sendError sends an error response to the client
func (s *Server) sendError(conn net.Conn, errMsg string) {
	if err := conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		return
	}

	errorResp := resp.SerializeError(errMsg)
	conn.Write(errorResp)
}
