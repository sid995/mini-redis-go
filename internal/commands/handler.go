package commands

import (
	"fmt"
	"strconv"

	"github.com/sid995/mini-redis/internal/resp"
	"github.com/sid995/mini-redis/internal/store"
)

// Handler handles Redis like commands and executes them against the store
type Handler struct {
	store *store.Store
	aof   AOFWriter
}

// AOFWriter is an interface for writing commands to AOF
type AOFWriter interface {
	WriteCommand(command []byte) error
}

// NewHandler creates a new command handler with the given store
func NewHandler(s *store.Store) *Handler {
	return &Handler{
		store: s,
		aof:   nil, // No AOF by default
	}
}

// NewHandlerWithAOF creates a new command handler with AOF support
func NewHandlerWithAOF(s *store.Store, aof AOFWriter) *Handler {
	return &Handler{
		store: s,
		aof:   aof,
	}
}

// SetAOF sets the AOF writer for the handler
func (h *Handler) SetAOF(aof AOFWriter) {
	h.aof = aof
}

// writeCommandToAOF writes a command to AOF if AOF is enabled
func (h *Handler) writeCommandToAOF(command string, args []string) {
	if h.aof == nil {
		return
	}

	// Serialize command as RESP array
	val := resp.Value{
		Type:  resp.Array,
		Array: make([]resp.Value, 0, len(args)+1),
	}

	// Add command name
	val.Array = append(val.Array, resp.Value{
		Type: resp.BulkString,
		Str:  command,
	})

	// Add arguments
	for _, arg := range args {
		val.Array = append(val.Array, resp.Value{
			Type: resp.BulkString,
			Str:  arg,
		})
	}

	// Serialize and write
	commandBytes, err := resp.Serialize(val)
	if err != nil {
		// Log error but don't fail the command
		return
	}

	h.aof.WriteCommand(commandBytes)
}

// Execute executes a command with the given arguments
// It returns the RESP formatted response as bytes
func (h *Handler) Execute(command string, args []string) []byte {
	switch command {
	case "PING":
		return h.ping(args)
	case "SET":
		return h.set(args)
	case "GET":
		return h.get(args)
	case "DEL":
		return h.del(args)
	case "EXPIRE":
		return h.expire(args)
	case "TTL":
		return h.ttl(args)
	case "QUIT":
		return h.quit()
	default:
		return h.error(fmt.Sprintf("ERR unknown command: %s", command))
	}
}

// ping handles the PING command
// PING [message] - Returns PONG or the message if provided
func (h *Handler) ping(args []string) []byte {
	if len(args) == 0 {
		return []byte("+PONG\r\n")
	}
	return []byte(fmt.Sprintf("+%s\r\n", args[0]))
}

// get handles the GET command
// GET key - Returns the value of key, or null if key doesn't exist
func (h *Handler) get(args []string) []byte {
	if len(args) != 1 {
		return h.error("ERR wrong number of arguments for 'get' command")
	}

	key := args[0]
	value, exists := h.store.Get(key)
	if !exists {
		return []byte("$-1\r\n")
	}

	// Serialize as bulk string
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), string(value)))
}

// set handles the SET command
// SET key value - Sets the value of key to value
func (h *Handler) set(args []string) []byte {
	if len(args) < 2 {
		return h.error("ERR wrong number of arguments for 'set' command")
	}

	key := args[0]
	value := []byte(args[1])
	h.store.Set(key, value)

	// Write to AOF
	h.writeCommandToAOF("SET", args)

	return []byte("+OK\r\n")
}

// del handles the DEL command
// DEL key - Deletes the key and returns the number of deleted keys
func (h *Handler) del(args []string) []byte {
	if len(args) == 0 {
		return h.error("ERR wrong number of arguments for 'del' command")
	}

	count := 0
	for _, key := range args {
		// check if the key exists before deleting
		_, exists := h.store.Get(key)
		if exists {
			h.store.Del(key)
			count++
		}
	}

	// Write to AOF only if keys were deleted
	if count > 0 {
		h.writeCommandToAOF("DEL", args)
	}

	// return number of keys deleted
	return []byte(fmt.Sprintf(":%d\r\n", count))
}

// expire handles the EXPIRE command
// EXPIRE key seconds - Sets a time-to-live (TTL) for the key
func (h *Handler) expire(args []string) []byte {
	if len(args) != 2 {
		return h.error("ERR wrong number of arguments for 'expire' command")
	}

	key := args[0]
	seconds, err := parseInt64(args[1])
	if err != nil {
		return h.error("ERR value is not an integer or out of range")
	}
	success := h.store.Expire(key, seconds)

	// Write to AOF if successful
	if success {
		h.writeCommandToAOF("EXPIRE", args)
	}

	if success {
		return []byte(":1\r\n") // 1 if timeout was set
	}
	return []byte(":0\r\n") // 0 if key doesn't exist
}

// ttl handles the TTL command
// TTL key - Returns the remaining time to live of a key in seconds
func (h *Handler) ttl(args []string) []byte {
	if len(args) != 1 {
		return h.error("ERR wrong number of arguments foer 'ttl' command")
	}

	key := args[0]
	ttl := h.store.TTL(key)

	// TTL returns: positive int (seconds), -1 (no TTL), -2 (key doesn't exist)
	return []byte(fmt.Sprintf(":%d\r\n", ttl))
}

// quit handles the QUIT command
// QUIT - closes the connection
func (h *Handler) quit() []byte {
	return []byte("+OK\r\n")
}

// error returns a RESP error message
func (h *Handler) error(msg string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", msg))
}

// parseInt64 parses a string to int64
func parseInt64(s string) (int64, error) {
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}
