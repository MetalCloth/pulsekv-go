// Package server owns the network lifecycle, client state, transactions,
// pub/sub coordination, and the replication control plane.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/persistence"
	"github.com/MetalCloth/pulsekv-go/internal/resp"
	"github.com/MetalCloth/pulsekv-go/internal/store"
)

type Config struct {
	Addr           string
	Dir            string
	DBFilename     string
	AppendOnly     bool
	AppendDir      string
	AppendFilename string
	AppendFsync    string
	ReplicaOf      string
}

func DefaultConfig() Config {
	return Config{
		Addr:           ":6379",
		Dir:            ".",
		AppendDir:      "appendonlydir",
		AppendFilename: "appendonly.aof",
		AppendFsync:    "everysec",
	}
}

type User struct {
	Name      string
	Enabled   bool
	NoPass    bool
	Passwords map[[32]byte]struct{}
}

type Client struct {
	server        *Server
	conn          net.Conn
	writeMu       sync.Mutex
	multi         bool
	queue         [][]string
	watched       map[string]uint64
	subs          map[string]bool
	patterns      map[string]bool
	user          *User
	authenticated bool
	replica       bool
	closed        chan struct{}
	closeOnce     sync.Once
}

type replicaState struct {
	client    *Client
	ackOffset int64
}

type Server struct {
	cfg       Config
	store     *store.Store
	listener  net.Listener
	closeOnce sync.Once
	closed    chan struct{}
	clientsMu sync.Mutex
	clients   map[*Client]struct{}

	mutationMu sync.Mutex
	usersMu    sync.RWMutex
	users      map[string]*User

	pubMu    sync.Mutex
	channels map[string]map[*Client]struct{}
	patterns map[string]map[*Client]struct{}

	replMu       sync.Mutex
	replID       string
	masterReplID string
	replOffset   int64
	replicas     map[*Client]*replicaState
	ackChanged   chan struct{}
	replicaConn  net.Conn

	aof *persistence.AOF
}

func New(config Config) (*Server, error) {
	if config.Addr == "" {
		config.Addr = ":6379"
	}
	if config.AppendFsync == "" {
		config.AppendFsync = "everysec"
	}
	if config.AppendFsync != "always" && config.AppendFsync != "everysec" && config.AppendFsync != "no" {
		return nil, fmt.Errorf("invalid appendfsync mode %q", config.AppendFsync)
	}
	replID := make([]byte, 20)
	if _, err := rand.Read(replID); err != nil {
		return nil, err
	}
	s := &Server{
		cfg:        config,
		store:      store.New(),
		closed:     make(chan struct{}),
		clients:    make(map[*Client]struct{}),
		users:      make(map[string]*User),
		channels:   make(map[string]map[*Client]struct{}),
		patterns:   make(map[string]map[*Client]struct{}),
		replID:     hex.EncodeToString(replID),
		replicas:   make(map[*Client]*replicaState),
		ackChanged: make(chan struct{}),
	}
	s.users["default"] = &User{Name: "default", Enabled: true, NoPass: true, Passwords: make(map[[32]byte]struct{})}

	if config.DBFilename != "" {
		path := config.DBFilename
		if config.Dir != "" && !strings.HasPrefix(path, "/") {
			path = config.Dir + string(os.PathSeparator) + path
		}
		if err := persistence.LoadRDB(path, s.store); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("load RDB: %w", err)
		}
	}
	if config.AppendOnly {
		aof, err := persistence.OpenAOF(persistence.AOFConfig{
			Dir: config.Dir, AppendDir: config.AppendDir, AppendFile: config.AppendFilename,
			AppendFsync: config.AppendFsync,
			Replay: func(command []string) error {
				value, _ := s.execute(command, nil, true, false)
				if value.Kind == resp.Error {
					return fmt.Errorf("AOF command %q failed: %s", command, value.Bytes)
				}
				return nil
			},
		})
		if err != nil {
			return nil, fmt.Errorf("open AOF: %w", err)
		}
		s.aof = aof
	}
	return s, nil
}

func (s *Server) Store() *store.Store { return s.store }

func (s *Server) Addr() string {
	if s.listener == nil {
		return s.cfg.Addr
	}
	return s.listener.Addr().String()
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.closed:
		}
	}()
	if s.cfg.ReplicaOf != "" {
		go s.replicaLoop()
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}
		client := &Client{
			server: s, conn: conn, watched: make(map[string]uint64),
			subs: make(map[string]bool), patterns: make(map[string]bool), closed: make(chan struct{}),
		}
		s.usersMu.RLock()
		client.user = s.users["default"]
		client.authenticated = client.user.NoPass
		s.usersMu.RUnlock()
		s.clientsMu.Lock()
		s.clients[client] = struct{}{}
		s.clientsMu.Unlock()
		go s.serve(client)
	}
}

func (s *Server) serve(client *Client) {
	defer s.removeClient(client)
	reader := resp.NewReader(client.conn)
	for {
		value, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			_ = client.send(resp.ErrorValue("ERR " + err.Error()))
			return
		}
		command, err := resp.Command(value)
		if err != nil {
			_ = client.send(resp.ErrorValue("ERR " + err.Error()))
			return
		}
		responses, closeAfter := s.handleClientCommand(client, command)
		for _, response := range responses {
			if response.Kind == 0 {
				continue
			}
			if err := client.send(response); err != nil {
				return
			}
		}
		if closeAfter {
			return
		}
	}
}

func (c *Client) send(value resp.Value) error { return c.sendRaw(resp.Encode(value)) }

func (c *Client) sendRaw(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (s *Server) removeClient(client *Client) {
	client.closeOnce.Do(func() {
		close(client.closed)
		_ = client.conn.Close()
	})
	s.clientsMu.Lock()
	delete(s.clients, client)
	s.clientsMu.Unlock()
	s.removeSubscriptions(client)
	s.replMu.Lock()
	if _, wasReplica := s.replicas[client]; wasReplica {
		delete(s.replicas, client)
		close(s.ackChanged)
		s.ackChanged = make(chan struct{})
	}
	s.replMu.Unlock()
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.clientsMu.Lock()
		clients := make([]*Client, 0, len(s.clients))
		for client := range s.clients {
			clients = append(clients, client)
		}
		s.clientsMu.Unlock()
		for _, client := range clients {
			_ = client.conn.Close()
		}
		s.replMu.Lock()
		upstream := s.replicaConn
		s.replMu.Unlock()
		if upstream != nil {
			_ = upstream.Close()
		}
		if closeErr := s.aof.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}

func (s *Server) handleClientCommand(client *Client, command []string) (responses []resp.Value, closeAfter bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			responses = []resp.Value{resp.ErrorValue("ERR internal command failure")}
			closeAfter = false
		}
	}()
	if len(command) == 0 {
		return []resp.Value{resp.ErrorValue("ERR empty command")}, false
	}
	name := strings.ToUpper(command[0])
	if !client.authenticated && name != "AUTH" && name != "QUIT" {
		return []resp.Value{resp.ErrorValue("NOAUTH Authentication required.")}, false
	}
	if client.inSubscriptionMode() && !isSubscriptionCommand(name) {
		return []resp.Value{resp.ErrorValue("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context")}, false
	}

	switch name {
	case "MULTI":
		if client.multi {
			return []resp.Value{resp.ErrorValue("ERR MULTI calls can not be nested")}, false
		}
		client.multi = true
		return []resp.Value{resp.Simple("OK")}, false
	case "EXEC":
		return []resp.Value{s.execTransaction(client)}, false
	case "DISCARD":
		if !client.multi {
			return []resp.Value{resp.ErrorValue("ERR DISCARD without MULTI")}, false
		}
		client.multi, client.queue, client.watched = false, nil, make(map[string]uint64)
		return []resp.Value{resp.Simple("OK")}, false
	case "WATCH":
		if client.multi {
			return []resp.Value{resp.ErrorValue("ERR WATCH inside MULTI is not allowed")}, false
		}
		for _, key := range command[1:] {
			client.watched[key] = s.store.Version(key)
		}
		return []resp.Value{resp.Simple("OK")}, false
	case "UNWATCH":
		client.watched = make(map[string]uint64)
		return []resp.Value{resp.Simple("OK")}, false
	case "QUIT":
		return []resp.Value{resp.Simple("OK")}, true
	}

	if client.multi {
		client.queue = append(client.queue, append([]string(nil), command...))
		return []resp.Value{resp.Simple("QUEUED")}, false
	}
	if name == "SUBSCRIBE" || name == "UNSUBSCRIBE" || name == "PSUBSCRIBE" || name == "PUNSUBSCRIBE" {
		return s.subscriptionCommand(client, command), false
	}
	if name == "PSYNC" {
		return []resp.Value{s.handlePSYNC(client, command)}, false
	}
	response, _ := s.execute(command, client, false, false)
	return []resp.Value{response}, false
}

func (s *Server) execTransaction(client *Client) resp.Value {
	if !client.multi {
		return resp.ErrorValue("ERR EXEC without MULTI")
	}
	defer func() {
		client.multi = false
		client.queue = nil
		client.watched = make(map[string]uint64)
	}()
	for key, version := range client.watched {
		if s.store.Version(key) != version {
			return resp.NullArrayValue()
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	responses := make([]resp.Value, 0, len(client.queue))
	for _, command := range client.queue {
		response, _ := s.execute(command, client, false, true)
		responses = append(responses, response)
	}
	return resp.ArrayValue(responses...)
}

func isSubscriptionCommand(name string) bool {
	switch name {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT":
		return true
	default:
		return false
	}
}

func (c *Client) inSubscriptionMode() bool { return len(c.subs) > 0 || len(c.patterns) > 0 }

func (s *Server) removeSubscriptions(client *Client) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	for channel := range client.subs {
		if subscribers := s.channels[channel]; subscribers != nil {
			delete(subscribers, client)
			if len(subscribers) == 0 {
				delete(s.channels, channel)
			}
		}
	}
	for pattern := range client.patterns {
		if subscribers := s.patterns[pattern]; subscribers != nil {
			delete(subscribers, client)
			if len(subscribers) == 0 {
				delete(s.patterns, pattern)
			}
		}
	}
	client.subs = make(map[string]bool)
	client.patterns = make(map[string]bool)
}

func commandFrame(command []string) []byte {
	values := make([]resp.Value, len(command))
	for i, arg := range command {
		values[i] = resp.BulkStringValue(arg)
	}
	return resp.Encode(resp.ArrayValue(values...))
}

func (s *Server) recordMutation(command []string) {
	if s.aof != nil {
		if err := s.aof.Append(command); err != nil {
			fmt.Fprintf(os.Stderr, "AOF append failed: %v\n", err)
		}
	}
	frame := commandFrame(command)
	s.replMu.Lock()
	s.replOffset += int64(len(frame))
	peers := make([]*Client, 0, len(s.replicas))
	for client := range s.replicas {
		peers = append(peers, client)
	}
	s.replMu.Unlock()
	for _, peer := range peers {
		if err := peer.sendRaw(frame); err != nil {
			s.removeReplica(peer)
			_ = peer.conn.Close()
		}
	}
}

func (s *Server) handlePSYNC(client *Client, command []string) resp.Value {
	if len(command) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for PSYNC")
	}
	// Keep offset capture, snapshot transfer, and registration in one mutation
	// interval so a write cannot land between the RDB and command stream.
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.replMu.Lock()
	id, offset := s.replID, s.replOffset
	s.replMu.Unlock()
	if err := client.send(resp.Simple(fmt.Sprintf("FULLRESYNC %s %d", id, offset))); err != nil {
		return resp.Value{}
	}
	rdb := persistence.EncodeStrings(s.store.StringSnapshot())
	if err := client.send(resp.Bulk(rdb)); err != nil {
		return resp.Value{}
	}
	client.replica = true
	s.replMu.Lock()
	s.replicas[client] = &replicaState{client: client, ackOffset: offset}
	s.replMu.Unlock()
	return resp.Value{}
}

func (s *Server) updateReplicaAck(client *Client, offset int64) {
	s.replMu.Lock()
	if state := s.replicas[client]; state != nil && offset > state.ackOffset {
		state.ackOffset = offset
		close(s.ackChanged)
		s.ackChanged = make(chan struct{})
	}
	s.replMu.Unlock()
}

func (s *Server) removeReplica(client *Client) {
	s.replMu.Lock()
	if _, ok := s.replicas[client]; ok {
		delete(s.replicas, client)
		close(s.ackChanged)
		s.ackChanged = make(chan struct{})
	}
	s.replMu.Unlock()
}

func (s *Server) waitForReplicas(required int, timeout time.Duration) int {
	if required <= 0 {
		return 0
	}
	s.replMu.Lock()
	target := s.replOffset
	s.replMu.Unlock()
	request := commandFrame([]string{"REPLCONF", "GETACK", "*"})
	s.replMu.Lock()
	peers := make([]*Client, 0, len(s.replicas))
	for client := range s.replicas {
		peers = append(peers, client)
	}
	s.replMu.Unlock()
	for _, peer := range peers {
		_ = peer.sendRaw(request)
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		s.replMu.Lock()
		acks := 0
		for _, state := range s.replicas {
			if state.ackOffset >= target {
				acks++
			}
		}
		changed := s.ackChanged
		s.replMu.Unlock()
		if acks >= required || acks >= len(peers) {
			return acks
		}
		select {
		case <-changed:
		case <-deadline.C:
			return acks
		case <-s.closed:
			return acks
		}
	}
}

func (s *Server) replicaLoop() {
	for {
		select {
		case <-s.closed:
			return
		default:
		}
		if err := s.replicateOnce(); err != nil {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-s.closed:
				return
			}
			continue
		}
		return
	}
}

func (s *Server) replicateOnce() error {
	addr := strings.TrimSpace(s.cfg.ReplicaOf)
	if strings.Contains(addr, " ") {
		parts := strings.Fields(addr)
		if len(parts) != 2 {
			return errors.New("replicaof must be HOST PORT")
		}
		addr = net.JoinHostPort(parts[0], parts[1])
	}
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	s.replMu.Lock()
	s.replicaConn = conn
	s.replMu.Unlock()
	defer func() {
		_ = conn.Close()
		s.replMu.Lock()
		if s.replicaConn == conn {
			s.replicaConn = nil
		}
		s.replMu.Unlock()
	}()
	reader := resp.NewReader(conn)
	for _, command := range [][]string{
		{"PING"},
		{"REPLCONF", "listening-port", portFromAddr(s.cfg.Addr)},
		{"REPLCONF", "capa", "psync2"},
	} {
		if err := writeCommand(conn, command); err != nil {
			return err
		}
		if _, err := reader.Read(); err != nil {
			return err
		}
	}
	if err := writeCommand(conn, []string{"PSYNC", "?", "-1"}); err != nil {
		return err
	}
	status, err := reader.Read()
	if err != nil {
		return err
	}
	line, ok := status.StringValue()
	if !ok || !strings.HasPrefix(line, "FULLRESYNC ") && line != "CONTINUE" {
		return fmt.Errorf("unexpected PSYNC response")
	}
	if status.Kind == resp.SimpleString && strings.HasPrefix(line, "FULLRESYNC ") {
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return errors.New("invalid FULLRESYNC response")
		}
		offset, parseErr := strconv.ParseInt(parts[2], 10, 64)
		if parseErr != nil || offset < 0 {
			return errors.New("invalid FULLRESYNC offset")
		}
		snapshot, err := reader.Read()
		if err != nil {
			return err
		}
		if snapshot.Kind != resp.BulkString || snapshot.Null {
			return errors.New("master sent invalid RDB snapshot")
		}
		if err := persistence.LoadRDBBytes(snapshot.Bytes, s.store); err != nil {
			return err
		}
		s.replMu.Lock()
		s.masterReplID = parts[1]
		s.replOffset = offset
		s.replMu.Unlock()
	}
	for {
		value, err := reader.Read()
		if err != nil {
			return err
		}
		command, err := resp.Command(value)
		if err != nil {
			return err
		}
		if len(command) >= 2 && strings.EqualFold(command[0], "REPLCONF") && strings.EqualFold(command[1], "GETACK") {
			offset := s.replicaOffset()
			if err := writeCommand(conn, []string{"REPLCONF", "ACK", strconv.FormatInt(offset, 10)}); err != nil {
				return err
			}
			continue
		}
		response, _ := s.execute(command, nil, true, false)
		if response.Kind == resp.Error {
			return fmt.Errorf("replication command failed: %s", response.Bytes)
		}
		s.replMu.Lock()
		s.replOffset += int64(len(resp.Encode(value)))
		s.replMu.Unlock()
	}
}

func (s *Server) replicaOffset() int64 {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	return s.replOffset
}

func writeCommand(conn net.Conn, command []string) error {
	data := commandFrame(command)
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func portFromAddr(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err == nil {
		return port
	}
	return "6379"
}
