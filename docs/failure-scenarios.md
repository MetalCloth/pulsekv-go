# Failure scenarios and responses

These are the incidents the implementation is designed to make reproducible.
Each one has a small regression test or a direct command sequence.

| Scenario | What would go wrong | Current response | Evidence / next step |
| --- | --- | --- | --- |
| Bulk value contains CRLF | line parser consumes part of the next command | declared byte length plus exact CRLF validation | `TestReaderPreservesBulkBytesAndPipelines`; fuzzing can extend malformed-frame coverage |
| `ECHO` has a missing argument | unchecked index panics the connection goroutine | arity validation returns `ERR`; connection remains usable | server command test; add a full malformed-command table if command surface grows |
| `SET` overwrites a key with an old TTL | new value disappears when the old deadline fires | `Store.Set` clears expiry before applying the replacement | `TestSetClearsPreviousExpiry` |
| producer wins a `BLPOP` race | waiter sleeps on a newly-created notification channel | capture channel before read and compare it before waiting | blocking integration test; stress with `-race` |
| expired list/stream/zset key remains in another map | `TYPE` and collection commands disagree | purge removes every type map and advances the version | store expiry paths; add mixed-type expiry fixtures when needed |
| transaction watches a deleted key | `EXEC` commits despite a delete | delete and expiry both touch the key version | WATCH integration test; add expiry/WATCH timing test |
| interrupted final AOF write | startup rejects an otherwise usable log | replay discards only an incomplete final frame | AOF replay test; add a truncated-frame fixture |
| AOF replays its own writes | restart loops or duplicates mutations | replay calls dispatcher with recording disabled | AOF recovery integration test |
| replica disconnects mid-broadcast | primary blocks forever or keeps a dead socket | failed send removes and closes that replica | replication code path; add forced-close integration fixture |
| replica applies `GETACK` as a data command | ACK handshake stalls `WAIT` | replica handles `REPLCONF GETACK` locally and replies with offset | replication/WAIT integration test |
| Pub/Sub and normal reply share a socket | bytes interleave and corrupt RESP | `Client.writeMu` serializes every write | Pub/Sub integration test; add pattern/unsubscribe matrix |
| password appears in logs | credential leaks through diagnostics | ACL stores only SHA-256 digests and command code emits no password logs | auth path review; add secret-redaction check if logging expands |

The global mutation lock is a conscious capacity ceiling: it gives simple,
atomic ordering for a learning server. If a benchmark shows contention, the
replacement should preserve the same invariants with per-key locking and an
explicit lock-order document.
