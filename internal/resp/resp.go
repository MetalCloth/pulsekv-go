// Package resp implements the RESP2 wire format used by Redis.
//
// The parser deliberately reads bulk payloads by their declared byte length.
// A line-oriented parser is tempting, but it corrupts perfectly valid values
// containing CR or LF and cannot safely process pipelined requests.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulkLength = 64 << 20
	MaxArrayItems = 1024
	MaxNesting    = 64
)

type Kind byte

const (
	SimpleString Kind = '+'
	Error        Kind = '-'
	Integer      Kind = ':'
	BulkString   Kind = '$'
	Array        Kind = '*'
)

// Value is a RESP2 value. NullArray distinguishes a null array from an empty
// array; Null bulk strings use BulkString with a nil Bytes value.
type Value struct {
	Kind      Kind
	Bytes     []byte
	Int       int64
	Array     []Value
	Null      bool
	NullArray bool
}

func Simple(s string) Value      { return Value{Kind: SimpleString, Bytes: []byte(s)} }
func ErrorValue(s string) Value  { return Value{Kind: Error, Bytes: []byte(s)} }
func IntegerValue(n int64) Value { return Value{Kind: Integer, Int: n} }
func Bulk(b []byte) Value {
	if b == nil {
		return Value{Kind: BulkString, Null: true}
	}
	return Value{Kind: BulkString, Bytes: append([]byte(nil), b...)}
}
func BulkStringValue(s string) Value   { return Bulk([]byte(s)) }
func ArrayValue(values ...Value) Value { return Value{Kind: Array, Array: values} }
func NullArrayValue() Value            { return Value{Kind: Array, NullArray: true} }

func (v Value) StringValue() (string, bool) {
	if v.Kind != BulkString && v.Kind != SimpleString {
		return "", false
	}
	if v.Null {
		return "", false
	}
	return string(v.Bytes), true
}

type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 32*1024)} }

func (r *Reader) Read() (Value, error) {
	return r.read(0)
}

func (r *Reader) read(depth int) (Value, error) {
	if depth > MaxNesting {
		return Value{}, errors.New("RESP nesting limit exceeded")
	}
	prefix, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Kind(prefix) {
	case SimpleString, Error, Integer:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		if prefix == ':' {
			n, err := strconv.ParseInt(string(line), 10, 64)
			if err != nil {
				return Value{}, fmt.Errorf("invalid RESP integer: %w", err)
			}
			return IntegerValue(n), nil
		}
		return Value{Kind: Kind(prefix), Bytes: line}, nil
	case BulkString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := parseLength(line)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Bulk(nil), nil
		}
		if n > MaxBulkLength {
			return Value{}, fmt.Errorf("RESP bulk string exceeds %d bytes", MaxBulkLength)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r.r, payload); err != nil {
			return Value{}, fmt.Errorf("read bulk payload: %w", err)
		}
		if err := r.expectCRLF(); err != nil {
			return Value{}, err
		}
		return Bulk(payload), nil
	case Array:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := parseLength(line)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return NullArrayValue(), nil
		}
		if n > MaxArrayItems {
			return Value{}, fmt.Errorf("RESP array exceeds %d items", MaxArrayItems)
		}
		values := make([]Value, n)
		for i := range values {
			values[i], err = r.read(depth + 1)
			if err != nil {
				return Value{}, err
			}
		}
		return ArrayValue(values...), nil
	default:
		return Value{}, fmt.Errorf("unsupported RESP type %q", prefix)
	}
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("RESP line is not terminated by CRLF")
	}
	return append([]byte(nil), line[:len(line)-2]...), nil
}

func (r *Reader) expectCRLF() error {
	var suffix [2]byte
	if _, err := io.ReadFull(r.r, suffix[:]); err != nil {
		return err
	}
	if suffix != [2]byte{'\r', '\n'} {
		return errors.New("RESP bulk string is not terminated by CRLF")
	}
	return nil
}

func parseLength(line []byte) (int, error) {
	if len(line) == 0 {
		return 0, errors.New("empty RESP length")
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil || n < -1 {
		return 0, fmt.Errorf("invalid RESP length %q", line)
	}
	if n > int64(^uint(0)>>1) {
		return 0, errors.New("RESP length overflows int")
	}
	return int(n), nil
}

func Encode(v Value) []byte {
	var b strings.Builder
	encodeInto(&b, v)
	return []byte(b.String())
}

func encodeInto(b *strings.Builder, v Value) {
	switch v.Kind {
	case SimpleString, Error:
		b.WriteByte(byte(v.Kind))
		b.Write(v.Bytes)
		b.WriteString("\r\n")
	case Integer:
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(v.Int, 10))
		b.WriteString("\r\n")
	case BulkString:
		if v.Null {
			b.WriteString("$-1\r\n")
			return
		}
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(v.Bytes)))
		b.WriteString("\r\n")
		b.Write(v.Bytes)
		b.WriteString("\r\n")
	case Array:
		if v.NullArray {
			b.WriteString("*-1\r\n")
			return
		}
		b.WriteByte('*')
		b.WriteString(strconv.Itoa(len(v.Array)))
		b.WriteString("\r\n")
		for _, child := range v.Array {
			encodeInto(b, child)
		}
	default:
		panic(fmt.Sprintf("unsupported RESP kind %q", v.Kind))
	}
}

func Command(v Value) ([]string, error) {
	if v.Kind != Array || v.NullArray {
		return nil, errors.New("command must be a non-null RESP array")
	}
	cmd := make([]string, len(v.Array))
	for i, item := range v.Array {
		s, ok := item.StringValue()
		if !ok {
			return nil, fmt.Errorf("command argument %d must be a string", i)
		}
		cmd[i] = s
	}
	if len(cmd) == 0 {
		return nil, errors.New("empty command")
	}
	return cmd, nil
}
