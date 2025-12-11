package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Writer handles writing commands to an Append Only File (AOF)
// it provides thread safe appending of commands for durability
type Writer struct {
	file         *os.File
	writer       *bufio.Writer
	mu           sync.Mutex
	syncInterval time.Duration // interval at which to sync file to disk
	lastSync     time.Time     // time of the last sync
}

// Config holds configuration for the AOF writer
type Config struct {
	FilePath     string        // path to the AOF file
	SyncInterval time.Duration // how often to sync to disk (0 = sync on every write)
	AutoSync     bool          // enables automatic periodic syncing
}

// DefaultConfig returns a default AOF configuration
func DefaultConfig() *Config {
	return &Config{
		FilePath:     "appendonly.aof",
		SyncInterval: time.Second,
		AutoSync:     true,
	}
}

// NewWriter creates a new AOF writer with the given configuration
func NewWriter(config *Config) (*Writer, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Open file in append mode, create if it doesn't exist
	file, err := os.OpenFile(config.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644) // 0644 is the file mode
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}

	writer := &Writer{
		file:         file,
		writer:       bufio.NewWriter(file),
		syncInterval: config.SyncInterval,
		lastSync:     time.Now(),
	}

	// Start auto sync goroutine if enabled
	if config.AutoSync && config.SyncInterval > 0 {
		go writer.autoSync()
	}

	return writer, nil
}

// WriteCommand appends a command to the AOF file
// The command is written in RESP format for easy replay
func (w *Writer) WriteCommand(command []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write the command
	if _, err := w.writer.Write(command); err != nil {
		return fmt.Errorf("failed to write command to AOF: %w", err)
	}

	// Flush the buffer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF buffer: %w", err)
	}

	// Sync immediately if sync interval is 0, otherwise let auto sync handle it
	if w.syncInterval == 0 {
		return w.sync()
	}

	return nil
}

// Sync forces a sync of the AOF file to disk
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

// sync performs the actual file sync (must be called with lock held)
func (w *Writer) sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// autoSync periodically syncs the AOF file to disk
func (w *Writer) autoSync() {
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for range ticker.C {
		w.mu.Lock()
		now := time.Now()
		// only sync if enough time has passed
		if now.Sub(w.lastSync) >= w.syncInterval {
			w.sync()
			w.lastSync = now
		}
		w.mu.Unlock()
	}
}

// Close closes AOF file and flushes any pending writes
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// Reader handles reading commands from an AOF file for recovery
type Reader struct {
	file   *os.File
	reader *bufio.Reader
}

// NewReader creates a new AOF reader for the given file path
func NewReader(filePath string) (*Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}
	return &Reader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

// ReadCommand reads the next command from the AOF file
// It returns the command bytes and any error encountered
func (r *Reader) ReadCommand() ([]byte, error) {
	if r == nil {
		return nil, io.EOF
	}

	// Read the RESP array
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 {
		return nil, fmt.Errorf("invalid AOF format: line too short")
	}

	// Remove \n (and \r if present)
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("invalid AOF format: empty line")
	}
	// Check if it's an array
	if line[0] != '*' {
		return nil, fmt.Errorf("invalid AOF format: expected array, got %c", line[0])
	}

	// read the array count
	arrayCount := 0
	for i := 1; i < len(line); i++ {
		if line[i] < '0' || line[i] > '9' {
			return nil, fmt.Errorf("invalid AOF format: expected digit, got %c", line[i])
		}
		arrayCount = arrayCount*10 + int(line[i]-'0')
	}

	// Read the full command (array header + elements)
	command := make([]byte, 0, 1024)
	command = append(command, line...)
	command = append(command, '\r', '\n')

	// Read array elements
	for i := 0; i < arrayCount; i++ {
		// Read bulk string
		bulkLine, err := r.reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		command = append(command, bulkLine...)

		// Read the actual string content
		if len(bulkLine) >= 2 && bulkLine[0] == '$' {
			// Parse length
			length := 0
			for j := 1; j < len(bulkLine)-2; j++ {
				if bulkLine[j] >= '0' && bulkLine[j] <= '9' {
					length = length*10 + int(bulkLine[j]-'0')
				}
			}

			// Read the string content + \r\n
			content := make([]byte, length+2)
			if _, err := io.ReadFull(r.reader, content); err != nil {
				return nil, err
			}
			command = append(command, content...)
		}
	}

	return command, nil
}

// Close closes the AOF reader
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	return r.file.Close()
}

// Replay replays all commands from the AOF file
// It calls the provided function for each command
func (r *Reader) Replay(fn func([]byte) error) error {
	if r == nil {
		return nil
	}

	for {
		command, err := r.ReadCommand()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("error reading AOF command: %w", err)
		}

		if err := fn(command); err != nil {
			return fmt.Errorf("error replaying AOF command: %w", err)
		}
	}

	return nil
}
