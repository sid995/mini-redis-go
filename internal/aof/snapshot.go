package aof

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SnapshotWriter handles writing snapshots of the srtore to disk
// Snapshots are written atomically using a temporary file and rename
type SnapshotWriter struct {
	filePath string
	mu       sync.Mutex
}

// NewSnapshotWriter creates a new snapshot writer
func NewSnapshotWriter(filePath string) *SnapshotWriter {
	return &SnapshotWriter{
		filePath: filePath,
	}
}

// NewSnapshotWriter writes a complete snapshot of the store to disk atomically
// It takes a function that writes the store data to the provided writer
func (sw *SnapshotWriter) WriteSnapshot(writeFn func(io.Writer) error) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Create temporary file in the same directory
	dir := filepath.Dir(sw.filePath)
	tmpFile := filepath.Join(dir, fmt.Sprintf(".snapshot.%d.tmp", time.Now().UnixNano()))

	// Create the temporary file
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Write snapshot data
	if err := writeFn(file); err != nil {
		os.Remove(tmpFile) // Cleanup error
		return fmt.Errorf("failed to write snapshot data: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		os.Remove(tmpFile) // Cleanup error
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		os.Remove(tmpFile) // Cleanup error
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	// Atomically rename temporary file to final location
	if err := os.Rename(tmpFile, sw.filePath); err != nil {
		os.Remove(tmpFile) // Cleanup error
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

// New

// SnapshotReader handles reading snapshots from disk
type SnapshotReader struct {
	filePath string
}

// NewSnapshotReader creates a new snapshot reader
func NewSnapshotReader(filePath string) *SnapshotReader {
	return &SnapshotReader{
		filePath: filePath,
	}
}

// ReadSnapshot reads a snapshot from disk and calls the provided function with the data
func (sr *SnapshotReader) ReadSnapshot(readFn func(io.Reader) error) error {
	file, err := os.Open(sr.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No snapshot exists
		}
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}

	defer file.Close()

	return readFn(file)
}

// Exists checks if a snapshot file exists
func (sr *SnapshotReader) Exists() bool {
	_, err := os.Stat(sr.filePath)
	return err == nil
}

// StoreSnapshot represents the serializablr state of the store
type StoreSnapshot struct {
	// Entries is a map of key to entry data
	Entries map[string]EntrySnapshot
}

// EntrySnapshot represents a single key-value entry with expiration
type EntrySnapshot struct {
	Value   []byte
	Expires int64
}

// WriteStoreSnapshot writes the store state to a writer using gob encoding
func WriteStoreSnapshot(w io.Writer, getEntries func() map[string]EntrySnapshot) error {
	entries := getEntries()
	snapshot := StoreSnapshot{
		Entries: entries,
	}

	encoder := gob.NewEncoder(w)
	return encoder.Encode(snapshot)
}

// ReadStoreSnapshot reads the store state from a reader using gob decoding
func ReadStoreSnapshot(r io.Reader) (*StoreSnapshot, error) {
	var snapshot StoreSnapshot
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	return &snapshot, nil
}
