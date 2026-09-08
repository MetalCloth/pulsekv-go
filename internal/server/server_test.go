package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/resp"
)

func startTestServer(t *testing.T, config Config) (*Server, string, context.CancelFunc) {
	t.Helper()
	if config.Addr == "" {
		config.Addr = freeAddress(t)
	}
	srv, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	addr := config.Addr
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			_ = srv.Close()
			t.Fatalf("server did not listen on %s: %v", addr, dialErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("server stopped with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("server did not stop")
		}
	})
	return srv, addr, cancel
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

type testClient struct {
	conn   net.Conn
	reader *resp.Reader
}

func dialTestClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client := &testClient{conn: conn, reader: resp.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	return client
}

func (c *testClient) command(t *testing.T, command ...string) resp.Value {
	t.Helper()
	values := make([]resp.Value, len(command))
	for i, argument := range command {
		values[i] = resp.BulkStringValue(argument)
	}
	if _, err := c.conn.Write(resp.Encode(resp.ArrayValue(values...))); err != nil {
		t.Fatal(err)
	}
	value, err := c.reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func requireSimple(t *testing.T, value resp.Value, expected string) {
	t.Helper()
	got, ok := value.StringValue()
	if !ok || value.Kind != resp.SimpleString || got != expected {
		t.Fatalf("got %#v, want simple %q", value, expected)
	}
}

func requireBulk(t *testing.T, value resp.Value, expected string) {
	t.Helper()
	got, ok := value.StringValue()
	if !ok || value.Kind != resp.BulkString || got != expected {
		t.Fatalf("got %#v, want bulk %q", value, expected)
	}
}

func requireInteger(t *testing.T, value resp.Value, expected int64) {
	t.Helper()
	if value.Kind != resp.Integer || value.Int != expected {
		t.Fatalf("got %#v, want integer %d", value, expected)
	}
}

func TestServerCoreCommandsAndPipelining(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{Addr: freeAddress(t)})
	client := dialTestClient(t, addr)
	requireSimple(t, client.command(t, "PING"), "PONG")
	requireBulk(t, client.command(t, "ECHO", "hello\r\nworld"), "hello\r\nworld")
	requireSimple(t, client.command(t, "SET", "name", "redis"), "OK")
	requireBulk(t, client.command(t, "GET", "name"), "redis")
	requireInteger(t, client.command(t, "EXPIRE", "name", "10"), 1)
	if ttl := client.command(t, "TTL", "name"); ttl.Kind != resp.Integer || ttl.Int < 0 {
		t.Fatalf("unexpected TTL: %#v", ttl)
	}
	requireSimple(t, client.command(t, "SET", "name", "replacement"), "OK")
	requireInteger(t, client.command(t, "TTL", "name"), -1)

	// Two RESP frames in one write must produce two independent replies.
	frames := resp.Encode(resp.ArrayValue(resp.BulkStringValue("PING")))
	frames = append(frames, resp.Encode(resp.ArrayValue(resp.BulkStringValue("PING"), resp.BulkStringValue("payload")))...)
	if _, err := client.conn.Write(frames); err != nil {
		t.Fatal(err)
	}
	requireSimple(t, readValue(t, client.reader), "PONG")
	requireBulk(t, readValue(t, client.reader), "payload")
}

func TestServerListsStreamsSortedSetsBitmapsAndGeo(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{Addr: freeAddress(t)})
	client := dialTestClient(t, addr)
	requireInteger(t, client.command(t, "LPUSH", "queue", "a", "b"), 2)
	requireInteger(t, client.command(t, "RPUSH", "queue", "c"), 3)
	list := client.command(t, "LRANGE", "queue", "-2", "-1")
	if len(list.Array) != 2 || string(list.Array[0].Bytes) != "a" || string(list.Array[1].Bytes) != "c" {
		t.Fatalf("unexpected list range: %#v", list)
	}
	requireInteger(t, client.command(t, "LLEN", "queue"), 3)
	if value := client.command(t, "LPOP", "queue", "2"); len(value.Array) != 2 {
		t.Fatalf("unexpected list pop: %#v", value)
	}

	streamID := client.command(t, "XADD", "events", "1-0", "kind", "created")
	requireBulk(t, streamID, "1-0")
	entries := client.command(t, "XRANGE", "events", "-", "+")
	if len(entries.Array) != 1 || len(entries.Array[0].Array) != 2 {
		t.Fatalf("unexpected stream range: %#v", entries)
	}
	read := client.command(t, "XREAD", "STREAMS", "events", "0-0")
	if len(read.Array) != 1 {
		t.Fatalf("unexpected stream read: %#v", read)
	}

	requireInteger(t, client.command(t, "ZADD", "scores", "2", "b", "1", "a"), 2)
	withScores := client.command(t, "ZRANGE", "scores", "0", "-1", "WITHSCORES")
	if len(withScores.Array) != 4 || string(withScores.Array[0].Bytes) != "a" || string(withScores.Array[1].Bytes) != "1" {
		t.Fatalf("unexpected sorted set range: %#v", withScores)
	}
	requireInteger(t, client.command(t, "ZRANK", "scores", "b"), 1)
	requireInteger(t, client.command(t, "ZCARD", "scores"), 2)

	requireInteger(t, client.command(t, "SETBIT", "bitmap", "9", "1"), 0)
	requireInteger(t, client.command(t, "GETBIT", "bitmap", "9"), 1)
	requireInteger(t, client.command(t, "BITCOUNT", "bitmap"), 1)
	requireInteger(t, client.command(t, "GEOADD", "cities", "77.5946", "12.9716", "bengaluru"), 1)
	geo := client.command(t, "GEOPOS", "cities", "bengaluru")
	if len(geo.Array) != 1 || len(geo.Array[0].Array) != 2 {
		t.Fatalf("unexpected geopos: %#v", geo)
	}
	if distance := client.command(t, "GEODIST", "cities", "bengaluru", "bengaluru", "km"); distance.Kind != resp.BulkString {
		t.Fatalf("unexpected geodist: %#v", distance)
	}
}

func TestServerTransactionsWatchBlockingAndPubSub(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{Addr: freeAddress(t)})
	client := dialTestClient(t, addr)
	requireSimple(t, client.command(t, "MULTI"), "OK")
	queued := client.command(t, "SET", "transaction-key", "value")
	queuedText, _ := queued.StringValue()
	if queuedText != "QUEUED" {
		t.Fatalf("unexpected queue response: %#v", queued)
	}
	transaction := client.command(t, "EXEC")
	if len(transaction.Array) != 1 {
		t.Fatalf("unexpected EXEC response: %#v", transaction)
	}

	watcher := dialTestClient(t, addr)
	mutator := dialTestClient(t, addr)
	requireSimple(t, watcher.command(t, "WATCH", "watched"), "OK")
	requireSimple(t, mutator.command(t, "SET", "watched", "changed"), "OK")
	requireSimple(t, watcher.command(t, "MULTI"), "OK")
	_ = watcher.command(t, "GET", "watched")
	if aborted := watcher.command(t, "EXEC"); !aborted.NullArray {
		t.Fatalf("WATCH did not abort transaction: %#v", aborted)
	}
	requireSimple(t, watcher.command(t, "SET", "expiring", "value", "PX", "20"), "OK")
	requireSimple(t, watcher.command(t, "WATCH", "expiring"), "OK")
	requireSimple(t, watcher.command(t, "MULTI"), "OK")
	_ = watcher.command(t, "GET", "expiring")
	time.Sleep(35 * time.Millisecond)
	if aborted := watcher.command(t, "EXEC"); !aborted.NullArray {
		t.Fatalf("WATCH did not observe passive expiry: %#v", aborted)
	}

	blpop := dialTestClient(t, addr)
	resultCh := make(chan resp.Value, 1)
	go func() { resultCh <- blpop.command(t, "BLPOP", "blocked", "1") }()
	time.Sleep(30 * time.Millisecond)
	requireInteger(t, client.command(t, "RPUSH", "blocked", "wake"), 1)
	select {
	case result := <-resultCh:
		if len(result.Array) != 2 || string(result.Array[1].Bytes) != "wake" {
			t.Fatalf("unexpected BLPOP result: %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("BLPOP was not woken by RPUSH")
	}

	subscriber := dialTestClient(t, addr)
	subscription := subscriber.command(t, "SUBSCRIBE", "updates")
	if len(subscription.Array) != 3 || string(subscription.Array[0].Bytes) != "subscribe" {
		t.Fatalf("unexpected subscription reply: %#v", subscription)
	}
	requireInteger(t, client.command(t, "PUBLISH", "updates", "ready"), 1)
	message := readValue(t, subscriber.reader)
	if len(message.Array) != 3 || string(message.Array[0].Bytes) != "message" || string(message.Array[2].Bytes) != "ready" {
		t.Fatalf("unexpected pub/sub message: %#v", message)
	}
}

func TestServerAuthentication(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{Addr: freeAddress(t)})
	admin := dialTestClient(t, addr)
	requireSimple(t, admin.command(t, "ACL", "SETUSER", "app", "resetpass", ">secret"), "OK")
	requireSimple(t, admin.command(t, "ACL", "SETUSER", "default", "resetpass", ">root-secret"), "OK")
	app := dialTestClient(t, addr)
	if denied := app.command(t, "GET", "key"); denied.Kind != resp.Error || !strings.Contains(string(denied.Bytes), "NOAUTH") {
		t.Fatalf("unauthenticated command was not denied: %#v", denied)
	}
	requireSimple(t, app.command(t, "AUTH", "app", "secret"), "OK")
	requireBulk(t, app.command(t, "ACL", "WHOAMI"), "app")
	if wrong := app.command(t, "AUTH", "app", "wrong"); wrong.Kind != resp.Error || !strings.Contains(string(wrong.Bytes), "WRONGPASS") {
		t.Fatalf("bad password was not rejected: %#v", wrong)
	}
	fresh := dialTestClient(t, addr)
	if denied := fresh.command(t, "PING"); denied.Kind != resp.Error || !strings.Contains(string(denied.Bytes), "NOAUTH") {
		t.Fatalf("default password did not enforce auth: %#v", denied)
	}
	requireSimple(t, fresh.command(t, "AUTH", "root-secret"), "OK")
}

func TestServerAOFRecovery(t *testing.T) {
	dir := t.TempDir()
	config := Config{Addr: freeAddress(t), Dir: dir, AppendOnly: true, AppendFsync: "always"}
	first, addr, cancel := startTestServer(t, config)
	client := dialTestClient(t, addr)
	requireSimple(t, client.command(t, "SET", "durable", "yes"), "OK")
	cancel()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondConfig := config
	secondConfig.Addr = freeAddress(t)
	second, secondAddr, _ := startTestServer(t, secondConfig)
	defer second.Close()
	requireBulk(t, dialTestClient(t, secondAddr).command(t, "GET", "durable"), "yes")
}

func TestServerReplicationAndWait(t *testing.T) {
	_, masterAddr, _ := startTestServer(t, Config{Addr: freeAddress(t)})
	masterClient := dialTestClient(t, masterAddr)
	requireSimple(t, masterClient.command(t, "SET", "snapshotted", "before-replica"), "OK")
	replicaConfig := Config{Addr: freeAddress(t), ReplicaOf: masterAddr}
	_, replicaAddr, _ := startTestServer(t, replicaConfig)
	replicaClient := dialTestClient(t, replicaAddr)

	// The replica performs a full sync asynchronously; polling keeps the test
	// independent of scheduler timing while still exercising the wire handshake.
	deadline := time.Now().Add(3 * time.Second)
	for {
		value := replicaClient.command(t, "GET", "snapshotted")
		if got, ok := value.StringValue(); ok && got == "before-replica" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica did not load the RDB snapshot: %#v", value)
		}
		time.Sleep(20 * time.Millisecond)
	}
	info := replicaClient.command(t, "INFO", "replication")
	infoText, _ := info.StringValue()
	if !strings.Contains(infoText, "role:slave") || !strings.Contains(infoText, "master_replid:") {
		t.Fatalf("replica INFO is missing replication state: %q", infoText)
	}
	requireInteger(t, masterClient.command(t, "WAIT", "1", "2000"), 1)
	requireSimple(t, masterClient.command(t, "SET", "replicated", "one"), "OK")
	deadline = time.Now().Add(3 * time.Second)
	for {
		value := replicaClient.command(t, "GET", "replicated")
		if got, ok := value.StringValue(); ok && got == "one" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica did not receive SET: %#v", value)
		}
		time.Sleep(20 * time.Millisecond)
	}
	requireInteger(t, masterClient.command(t, "WAIT", "1", "2000"), 1)
	requireInteger(t, masterClient.command(t, "RPUSH", "replicated-list", "value"), 1)
	deadline = time.Now().Add(3 * time.Second)
	for {
		value := replicaClient.command(t, "LRANGE", "replicated-list", "0", "-1")
		if len(value.Array) == 1 && string(value.Array[0].Bytes) == "value" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica did not receive RPUSH: %#v", value)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readValue(t *testing.T, reader *resp.Reader) resp.Value {
	t.Helper()
	value, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
