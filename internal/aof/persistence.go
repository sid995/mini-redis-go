package aof

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/sid995/mini-redis/internal/resp"
	"github.com/sid995/mini-redis/internal/store"
)

// PersistenceManager manages both AOF and snapshot persistence
type PersistenceManager struct {
	aofWriter      *Writer
	snapshotWriter *SnapshotWriter
	snapshotReader *SnapshotReader
	store          *store.Store
	handler        CommandExecutor
}

// CommandExecutor is an interface for executing commands during recovery
type CommandExecutor interface {
	Execute(command string, args []string) []byte
}

// Config holds configuration for persistence
type PersistenceConfig struct {
	// AOFConfig is the configuration for AOF
	AOFConfig *Config
	// SnapshotPath is the path to the snapshot file
	SnapshotPath string
	// EnableAOF enables AOF persistence
	EnableAOF bool
	// EnableSnapshot enables snapshot persistence
	EnableSnapshot bool
}

// DefaultPersistenceConfig returns a default persistence configuration
func DefaultPersistenceConfig() *PersistenceConfig {
	return &PersistenceConfig{
		AOFConfig:      DefaultConfig(),
		SnapshotPath:   "dump.rdb",
		EnableAOF:      true,
		EnableSnapshot: true,
	}
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(store *store.Store, handler CommandExecutor, config *PersistenceConfig) (*PersistenceManager, error) {
	if config == nil {
		config = DefaultPersistenceConfig()
	}

	pm := &PersistenceManager{
		store:   store,
		handler: handler,
	}

	// Initialize AOF if enabled
	if config.EnableAOF {
		aofWriter, err := NewWriter(config.AOFConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create AOF writer: %w", err)
		}
		pm.aofWriter = aofWriter
	}

	// Initialize snapshot if enabled
	if config.EnableSnapshot {
		pm.snapshotWriter = NewSnapshotWriter(config.SnapshotPath)
		pm.snapshotReader = NewSnapshotReader(config.SnapshotPath)
	}

	return pm, nil
}

// Recover loads data from snapshot and AOF files
func (pm *PersistenceManager) Recover() error {
	log.Println("Starting recovery...")

	// First, load from snapshot if it exists
	if pm.snapshotReader != nil && pm.snapshotReader.Exists() {
		log.Println("Loading snapshot...")
		if err := pm.loadSnapshot(); err != nil {
			return fmt.Errorf("failed to load snapshot: %w", err)
		}
		log.Println("Snapshot loaded successfully")
	}

	// Then, replay AOF commands
	if pm.aofWriter != nil {
		aofReader, err := NewReader(pm.aofWriter.file.Name())
		if err != nil {
			return fmt.Errorf("failed to create AOF reader: %w", err)
		}
		if aofReader != nil {
			log.Println("Replaying AOF commands...")
			if err := pm.replayAOF(aofReader); err != nil {
				return fmt.Errorf("failed to replay AOF: %w", err)
			}
			log.Println("AOF replay completed")
		}
	}

	log.Println("Recovery completed")
	return nil
}

// loadSnapshot loads the store state from a snapshot file
func (pm *PersistenceManager) loadSnapshot() error {
	return pm.snapshotReader.ReadSnapshot(func(r io.Reader) error {
		snapshot, err := ReadStoreSnapshot(r)
		if err != nil {
			return err
		}

		// Convert snapshot entries to store format
		entries := make(map[string]store.EntrySnapshot)
		for key, entry := range snapshot.Entries {
			entries[key] = store.EntrySnapshot{
				Value:   entry.Value,
				Expires: entry.Expires,
			}
		}

		// Load into store
		pm.store.LoadFromSnapshot(entries)
		return nil
	})
}

// replayAOF replays all commands from the AOF file
func (pm *PersistenceManager) replayAOF(reader *Reader) error {
	return reader.Replay(func(commandBytes []byte) error {
		// Parse the command
		val, err := resp.Parse(bufio.NewReader(bytes.NewReader(commandBytes)))
		if err != nil {
			return err
		}

		// Extract command and arguments
		cmd, args, err := resp.ToCommand(val)
		if err != nil {
			return err
		}

		// Execute the command (but don't write to AOF during replay)
		pm.handler.Execute(cmd, args)
		return nil
	})
}

// WriteSnapshot creates a snapshot of the current store state
func (pm *PersistenceManager) WriteSnapshot() error {
	if pm.snapshotWriter == nil {
		return fmt.Errorf("snapshot not enabled")
	}

	return pm.snapshotWriter.WriteSnapshot(func(w io.Writer) error {
		entries := pm.store.GetAllEntries()

		// Convert to snapshot format
		snapshot := StoreSnapshot{
			Entries: make(map[string]EntrySnapshot),
		}
		for key, entry := range entries {
			snapshot.Entries[key] = EntrySnapshot{
				Value:   entry.Value,
				Expires: entry.Expires,
			}
		}

		return WriteStoreSnapshot(w, func() map[string]EntrySnapshot {
			return snapshot.Entries
		})
	})
}

// GetAOFWriter returns the AOF writer
func (pm *PersistenceManager) GetAOFWriter() *Writer {
	return pm.aofWriter
}

// Close closes the persistence manager and flushes all data
func (pm *PersistenceManager) Close() error {
	if pm.aofWriter != nil {
		if err := pm.aofWriter.Close(); err != nil {
			return fmt.Errorf("failed to close AOF writer: %w", err)
		}
	}
	return nil
}
