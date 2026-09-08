# CodeCrafters Build Your Own Redis checklist

This is the working acceptance ledger for the stages supplied in the project
brief. A checked item means the command path exists and is covered by unit or
TCP integration evidence. `~` means the learning-server compatibility surface
is present but the full Redis production behavior (for example RDB compaction
or partial replication backlogs) is intentionally deferred and called out in
the architecture documents.

Legend: `[x]` implemented, `[~]` implemented with a documented ceiling, `[ ]`
not started.

## Networking and core commands

- [x] Bind to a port
- [x] Respond to PING
- [x] Respond to multiple PINGs
- [x] Handle concurrent clients
- [x] Implement the ECHO command
- [x] Implement the SET & GET commands
- [x] Expiry

## Lists

- [x] Create a list
- [x] Append an element
- [x] Append multiple elements
- [x] List elements (positive indexes)
- [x] List elements (negative indexes)
- [x] Prepend elements
- [x] Query list length
- [x] Remove an element
- [x] Remove multiple elements
- [x] Blocking retrieval
- [x] Blocking retrieval with timeout

## Streams

- [x] The TYPE command
- [x] Create a stream
- [x] Validating entry IDs
- [x] Partially auto-generated IDs
- [x] Fully auto-generated IDs
- [x] Query entries from stream
- [x] Query with `-`
- [x] Query with `+`
- [x] Query single stream using XREAD
- [x] Query multiple streams using XREAD
- [x] Blocking reads
- [x] Blocking reads without timeout
- [x] Blocking reads using `$`

## Transactions

- [x] The INCR command (1/3)
- [x] The INCR command (2/3)
- [x] The INCR command (3/3)
- [x] The MULTI command
- [x] The EXEC command
- [x] Empty transaction
- [x] Queueing commands
- [x] Executing a transaction
- [x] The DISCARD command
- [x] Failures within transactions
- [x] Multiple transactions

## Optimistic locking

- [x] The WATCH command
- [x] WATCH inside transaction
- [x] Tracking key modifications
- [x] Watching multiple keys
- [x] Watching missing keys
- [x] The UNWATCH command
- [x] Unwatch on EXEC
- [x] Unwatch on DISCARD

## Replication

- [x] Configure listening port
- [x] The INFO command
- [x] The INFO command on a replica
- [x] Initial replication ID and offset
- [x] Send handshake (1/3)
- [x] Send handshake (2/3)
- [x] Send handshake (3/3)
- [x] Receive handshake (1/2)
- [x] Receive handshake (2/2)
- [x] Empty RDB transfer
- [x] Single-replica propagation
- [x] Multi-replica propagation
- [x] Command processing
- [x] ACKs with no commands
- [x] ACKs with commands
- [x] WAIT with no replicas
- [x] WAIT with no commands
- [x] WAIT with multiple commands
- [~] Reconnect uses safe FULLRESYNC; a bounded backlog/`CONTINUE` path is deferred

## RDB persistence

- [x] RDB file config
- [x] Read a key
- [x] Read a string value
- [x] Read multiple keys
- [x] Read multiple string values
- [x] Read value with expiry
- [~] Generated snapshots cover strings and expiry; collection RDB opcodes are deferred because AOF preserves collection mutations

## AOF persistence

- [x] Default AOF options
- [x] AOF options from flags
- [x] Create append-only directory
- [x] Create append-only file
- [x] Create manifest file
- [x] Write a single command
- [x] Write multiple commands
- [x] Filter write commands
- [x] Replay a single command
- [x] Replay multiple commands
- [~] Rewrite/compaction is deferred; the manifest and incremental file are stable for the learning workload

## Pub/Sub

- [x] Subscribe to a channel
- [x] Subscribe to multiple channels
- [x] Enter subscribed mode
- [x] PING in subscribed mode
- [x] Publish a message
- [x] Deliver messages
- [x] Unsubscribe

## Sorted sets

- [x] Create a sorted set
- [x] Add members
- [x] Retrieve member rank
- [x] List sorted set members
- [x] ZRANGE with negative indexes
- [x] Count sorted set members
- [x] Retrieve member score
- [x] Remove a member

## Bitmaps

- [x] Create a bitmap
- [x] Retrieve a bit
- [x] Read a string as bits
- [x] Read bits as a string
- [x] Grow a bitmap
- [x] Count set bits
- [x] AND two bitmaps
- [x] AND bitmaps of different lengths
- [x] OR two bitmaps

## Geospatial commands

- [x] Respond to GEOADD
- [x] Validate coordinates
- [x] Store a location
- [x] Calculate location score
- [x] Respond to GEOPOS
- [x] Decode coordinates
- [x] Calculate distance
- [x] Search within radius

## Authentication

- [x] Respond to ACL WHOAMI
- [x] Respond to ACL GETUSER
- [x] The nopass flag
- [x] The passwords property
- [x] Setting default user password
- [x] The AUTH command
- [x] Enforce authentication
- [x] Authenticate using AUTH

## Evidence commands

```bash
GOCACHE=/tmp/pulsekv-go-cache go test ./...
GOCACHE=/tmp/pulsekv-go-cache go test -race ./...
go vet ./...
```

`internal/server/server_test.go` starts real primary/replica listeners and
verifies pipelining, collections, transactions, blocking reads, Pub/Sub, AOF
restart, FULLRESYNC, propagation, and `WAIT`. The remaining `~` entries are
explicitly documented ceilings, so a reviewer can distinguish a deliberate
scope choice from an accidental omission.
