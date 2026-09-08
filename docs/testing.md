# Testing and verification

The project keeps one small test behind each non-trivial boundary:

| Test surface | What it protects |
| --- | --- |
| `internal/resp/resp_test.go` | exact bulk lengths, embedded CRLF, pipelining, malformed terminators, null encoding |
| `internal/store/store_test.go` | TTL clearing, negative list ranges, stream IDs, bitmap growth, sorted-set order |
| `internal/persistence/persistence_test.go` | RDB string/expiry round trip, AOF replay, fsync modes, manifest layout |
| `internal/server/server_test.go` | real TCP framing, core commands, collections, transactions/WATCH, blockers, Pub/Sub, AOF restart, replication/WAIT |

Run the deterministic checks with:

```bash
GOCACHE=/tmp/pulsekv-go-cache go test ./...
GOCACHE=/tmp/pulsekv-go-cache go test -race ./...
go vet ./...
```

The server tests bind loopback sockets. Some locked-down agent sandboxes deny
socket creation; in that environment the unit tests still run, while the
integration command must be run with loopback permission. GitHub Actions runs
the full suite on Ubuntu.

The test helper sends commands over a `net.Conn` and parses replies with the
same RESP reader used by the server. This catches a class of bugs that direct
dispatcher tests cannot: partial writes, asynchronous frames, blocked clients,
restart replay, and handshake ordering.

Before a release, the following manual smoke is useful:

```bash
make build
./bin/pulsekv -port 6379 -dir ./data -appendonly yes
redis-cli -p 6379 SET smoke ok
redis-cli -p 6379 INFO replication
```

The CodeCrafters stage mapping is maintained separately so a new command can
land with its test and its acceptance evidence in one commit.
