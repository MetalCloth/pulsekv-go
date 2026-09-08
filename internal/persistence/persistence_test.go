package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/store"
)

func TestRDBRoundTripForStringsAndExpiry(t *testing.T) {
	source := store.New()
	deadline := time.Now().Add(time.Minute)
	if _, err := source.Set("hello", []byte("world"), &deadline, false, false); err != nil {
		t.Fatal(err)
	}
	data := EncodeStrings(source.StringSnapshot())
	target := store.New()
	if err := LoadRDBBytes(data, target); err != nil {
		t.Fatal(err)
	}
	value, ok, err := target.Get("hello")
	if err != nil || !ok || string(value) != "world" {
		t.Fatalf("round trip: %q %v %v", value, ok, err)
	}
	if got := target.TTL("hello", time.Second); got < 0 {
		t.Fatalf("expiry was not restored: %d", got)
	}
}

func TestAOFReplayAndEverySecondSync(t *testing.T) {
	dir := t.TempDir()
	state := store.New()
	aof, err := OpenAOF(AOFConfig{Dir: dir, AppendFsync: "always", Replay: func(command []string) error {
		if len(command) == 3 && command[0] == "SET" {
			_, err := state.Set(command[1], []byte(command[2]), nil, false, false)
			return err
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := aof.Append([]string{"SET", "durable", "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := aof.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "appendonlydir", "appendonly.aof.1.incr.aof")
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("AOF was not written: %v", err)
	}
	replayed := store.New()
	replayAOF, err := OpenAOF(AOFConfig{Dir: dir, AppendFsync: "no", Replay: func(command []string) error {
		if len(command) == 3 && command[0] == "SET" {
			_, err := replayed.Set(command[1], []byte(command[2]), nil, false, false)
			return err
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer replayAOF.Close()
	value, ok, _ := replayed.Get("durable")
	if !ok || string(value) != "yes" {
		t.Fatalf("AOF replay lost value: %q %v", value, ok)
	}
}
