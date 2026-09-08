package persistence

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/resp"
)

type AOF struct {
	mu       sync.Mutex
	file     *os.File
	mode     string
	stop     chan struct{}
	done     chan struct{}
	closeOne sync.Once
}

type AOFConfig struct {
	Dir         string
	AppendDir   string
	AppendFile  string
	AppendFsync string
	Replay      func([]string) error
}

func OpenAOF(config AOFConfig) (*AOF, error) {
	if config.Dir == "" {
		config.Dir = "."
	}
	if config.AppendDir == "" {
		config.AppendDir = "appendonlydir"
	}
	if config.AppendFile == "" {
		config.AppendFile = "appendonly.aof"
	}
	if config.AppendFsync == "" {
		config.AppendFsync = "everysec"
	}
	dir := filepath.Join(config.Dir, config.AppendDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, config.AppendFile+".manifest")
	fileName := config.AppendFile + ".1.incr.aof"
	if data, err := os.ReadFile(manifestPath); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.Fields(line)
			if len(parts) >= 6 && parts[0] == "file" && parts[4] == "type" && parts[5] == "i" {
				fileName = parts[1]
				break
			}
		}
	} else if errors.Is(err, os.ErrNotExist) {
		manifest := "file " + fileName + " seq 1 type i\n"
		if err := os.WriteFile(manifestPath, []byte(manifest), 0o640); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	path := filepath.Join(dir, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	aof := &AOF{file: file, mode: config.AppendFsync, stop: make(chan struct{}), done: make(chan struct{})}
	if err := replay(file, config.Replay); err != nil {
		file.Close()
		return nil, err
	}
	if config.AppendFsync == "everysec" {
		go aof.syncLoop()
	}
	return aof, nil
}

func replay(file *os.File, apply func([]string) error) error {
	if apply == nil {
		return nil
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	reader := resp.NewReader(file)
	for {
		value, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// An interrupted final write is safe to discard; complete records
			// before it have already been applied.
			break
		}
		if err != nil {
			return err
		}
		cmd, err := resp.Command(value)
		if err != nil {
			return err
		}
		if err := apply(cmd); err != nil {
			return err
		}
	}
	_, err := file.Seek(0, io.SeekEnd)
	return err
}

func (a *AOF) Append(command []string) error {
	if a == nil {
		return nil
	}
	values := make([]resp.Value, len(command))
	for i, arg := range command {
		values[i] = resp.BulkStringValue(arg)
	}
	data := resp.Encode(resp.ArrayValue(values...))
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.file.Write(data); err != nil {
		return err
	}
	if a.mode == "always" {
		return a.file.Sync()
	}
	return nil
}

func (a *AOF) syncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(a.done)
	for {
		select {
		case <-ticker.C:
			a.mu.Lock()
			_ = a.file.Sync()
			a.mu.Unlock()
		case <-a.stop:
			return
		}
	}
}

func (a *AOF) Close() error {
	if a == nil {
		return nil
	}
	var err error
	a.closeOne.Do(func() {
		close(a.stop)
		if a.mode == "everysec" {
			<-a.done
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if syncErr := a.file.Sync(); syncErr != nil {
			err = syncErr
		}
		if closeErr := a.file.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}

func FormatAOFMode(mode string) string {
	if mode == "always" || mode == "everysec" || mode == "no" {
		return mode
	}
	return strconv.Quote(mode)
}
