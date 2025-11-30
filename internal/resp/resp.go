package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Type represents the RESP data type
type Type byte

const (
	// SimpleString represents a RESP simple string (starts with '+')
	SimpleString Type = '+'
	// Error represents a RESP error (starts with '-')
	Error Type = '-'
	// Integer represents a RESP integer (starts with ':')
	Integer Type = ':'
	// BulkString represents a RESP bulk string (starts with '$')
	BulkString Type = '$'
	// Array represents a RESP array (starts with '*')
	Array Type = '*'
)

// Value represents a RESP value that can be any RESP type
type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
}

// Parse reads and parses a RESP message from the reader
// It returns the parsed Value and any error encountered
func Parse(reader *bufio.Reader) (Value, error) {
	// Read the first byte to determine the type
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return Value{}, err
	}

	if len(line) < 1 {
		return Value{}, errors.New("invalid RESP format: line too short")
	}

	// Remove \n (and \r if present)
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return Value{}, errors.New("invalid RESP format: empty line")
	}

	typ := Type(line[0])
	content := string(line[1:])

	switch typ {
	case SimpleString:
		// Simple string: +OK\r\n
		return Value{Type: SimpleString, Str: content}, nil
	case Error:
		// Error: -ERR message\r\n
		return Value{Type: Error, Str: content}, nil
	case Integer:
		// Integer: :123\r\n
		intVal, err := strconv.ParseInt(content, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid integer: %w", err)
		}
		return Value{Type: Integer, Int: intVal}, nil
	case BulkString:
		// Bulk string: $5\r\nhello\r\n or $-1\r\n (null)
		length, err := strconv.Atoi(content)
		if err != nil {
			return Value{}, fmt.Errorf("invalid bulk string length: %w", err)
		}

		if length == -1 {
			// Null bulk string
			return Value{Type: BulkString, Str: ""}, nil
		}

		// Read the actual string content
		data := make([]byte, length+2) // +2 for \r\n
		if _, err := io.ReadFull(reader, data); err != nil {
			return Value{}, fmt.Errorf("failed to read bulk string: %w", err)
		}

		if data[length] != '\r' || data[length+1] != '\n' {
			return Value{}, errors.New("invalid bulk string terminator")
		}

		return Value{Type: BulkString, Str: string(data[:length])}, nil
	case Array:
		// Array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
		count, err := strconv.Atoi(content)
		if err != nil {
			return Value{}, fmt.Errorf("invalid array length: %w", err)
		}
		if count == -1 {
			// Null array
			return Value{Type: Array, Array: nil}, nil
		}
		if count < 0 {
			return Value{}, errors.New("invalid array count")
		}
		array := make([]Value, 0, count)
		for i := 0; i < count; i++ {
			val, err := Parse(reader)
			if err != nil {
				return Value{}, fmt.Errorf("failed to parse array element %d: %w", i, err)
			}
			array = append(array, val)
		}

		return Value{Type: Array, Array: array}, nil
	default:
		return Value{}, fmt.Errorf("unknown RESP type: %c", typ)
	}
}

// Serialize converts a value to its RESP wire format
// It returns the serialized bytes and any error encountered
func Serialize(val Value) ([]byte, error) {
	switch val.Type {
	case SimpleString:
		return []byte(fmt.Sprintf("+%s\r\n", val.Str)), nil
	case Error:
		return []byte(fmt.Sprintf("-%s\r\n", val.Str)), nil
	case Integer:
		return []byte(fmt.Sprintf(":%d\r\n", val.Int)), nil
	case BulkString:
		if val.Str == "" && len(val.Str) == 0 {
			// Check if this is a null bulk string
			// For simplicity, we'll treat empty string as null
			return []byte("$-1\r\n"), nil
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.Str), val.Str)), nil
	case Array:
		if val.Array == nil {
			return []byte("*-1\r\n"), nil
		}
		result := []byte(fmt.Sprintf("*%d\r\n", len(val.Array)))
		for _, elem := range val.Array {
			serialized, err := Serialize(elem)
			if err != nil {
				return nil, err
			}
			result = append(result, serialized...)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported RESP type: %d", val.Type)
	}
}

// SerializeString is a convenience function to serialize a simple string
func SerializeString(s string) []byte {
	val := Value{Type: SimpleString, Str: s}
	result, _ := Serialize(val)
	return result
}

// SerializeError is a convenience function to serialize an error
func SerializeError(err string) []byte {
	val := Value{Type: Error, Str: err}
	result, _ := Serialize(val)
	return result
}

// SerializeInteger is a convenience function to serialize an integer
func SerializeInteger(i int64) []byte {
	val := Value{Type: Integer, Int: i}
	result, _ := Serialize(val)
	return result
}

// SerializeBulkString is a convenience function to serialize a bulk string
func SerializeBulkString(s string) []byte {
	val := Value{Type: BulkString, Str: s}
	result, _ := Serialize(val)
	return result
}

// SerializeArray is a convenience function to serialize an array
func SerializeArray(elements []Value) []byte {
	val := Value{Type: Array, Array: elements}
	result, _ := Serialize(val)
	return result
}

// SerializeNull is a convenience function to serialize a null bulk string
func SerializeNull() []byte {
	return []byte("$-1\r\n")
}

// ToCommand extracts command name and arguments from RESP Array
// It returns the command name (uppercase), arguments, and any error
func ToCommand(val Value) (string, []string, error) {
	if val.Type != Array {
		return "", nil, errors.New("command must be a RESP array")
	}
	if len(val.Array) == 0 {
		return "", nil, errors.New("empty command array")
	}

	// First element should be the command name (bulk string)
	cmdVal := val.Array[0]
	if cmdVal.Type != BulkString {
		return "", nil, errors.New("command name must be a bulk string")
	}

	command := strings.ToUpper(cmdVal.Str)
	args := make([]string, 0, len(val.Array)-1)

	// remaining elements are arguments
	for i := 1; i < len(val.Array); i++ {
		argVal := val.Array[i]
		if argVal.Type != BulkString {
			return "", nil, fmt.Errorf("argument %d must be a bulk string", i-1)
		}
		args = append(args, argVal.Str)
	}
	return command, args, nil
}
