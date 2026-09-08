package resp

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReaderPreservesBulkBytesAndPipelines(t *testing.T) {
	input := []byte("*2\r\n$3\r\nSET\r\n$12\r\nhello\r\nworld\r\n*1\r\n$4\r\nPING\r\n")
	reader := NewReader(bytes.NewReader(input))
	first, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	command, err := Command(first)
	if err != nil {
		t.Fatal(err)
	}
	if command[1] != "hello\r\nworld" {
		t.Fatalf("bulk payload was changed: %q", command[1])
	}
	second, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	command, err = Command(second)
	if err != nil || len(command) != 1 || command[0] != "PING" {
		t.Fatalf("unexpected pipelined value: %#v", second)
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReaderRejectsBadBulkTerminator(t *testing.T) {
	reader := NewReader(bytes.NewBufferString("$3\r\nabcxx"))
	if _, err := reader.Read(); err == nil {
		t.Fatal("expected malformed bulk string error")
	}
}

func TestEncodeNullArrayAndBulk(t *testing.T) {
	value := ArrayValue(Bulk(nil), NullArrayValue(), IntegerValue(4))
	want := "*3\r\n$-1\r\n*-1\r\n:4\r\n"
	if got := string(Encode(value)); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
