# Protocol and command behavior

Pulsekv speaks RESP2. Requests are arrays whose elements are simple or bulk
strings; replies use simple strings, errors, integers, bulk strings, arrays,
and null values.

## Framing

```text
*2\r\n$4\r\nECHO\r\n$12\r\nhello\r\nworld\r\n
```

The second payload is twelve bytes even though it contains a CRLF. The reader
uses that length, then validates the payload terminator. It does not split on
newlines. Arrays are bounded to 1,024 items, bulk strings to 64 MiB, and nested
arrays to 64 levels so an untrusted client cannot grow parser memory without
bound.

Malformed frames return an `ERR` reply and close that connection. A malformed
command argument returns an `ERR` reply while leaving the connection usable;
this distinction is covered by the integration test that sends a bad `ECHO`
request followed by `PING`.

## Command families

The command dispatcher normalizes names case-insensitively and performs arity
and option validation before touching the store. Wrong-type errors are kept in
Redis's `WRONGTYPE` shape. Unknown commands are explicit errors rather than
silent no-ops.

Collection replies use deterministic ordering: lists preserve insertion order,
streams preserve entry order, and sorted sets sort by `(score, member)`.

## Transactions and watches

`MULTI` changes only the client state. Subsequent commands are validated for
the queue and receive `QUEUED`; their effects happen only inside `EXEC`.
Execution holds the mutation lock for the entire queue. A command error becomes
one element in the EXEC array, so later queued commands still run. If a watched
key's version changed, `EXEC` returns a null array and clears the watch state.

`DISCARD` clears both queue and watches. `UNWATCH` clears watches without
leaving transaction mode.

## Blocking mode

`BLPOP` and `BRPOP` accept one or more keys and a floating-point timeout. Zero
means wait indefinitely until a list value arrives or the server shuts down.
`XREAD BLOCK` follows the same change-notification path and supports zero,
finite millisecond timeouts, multiple streams, and `$` as “the current last
ID”.

## Pub/Sub mode

`SUBSCRIBE`/`PSUBSCRIBE` return subscription acknowledgements and put the
connection in subscribed mode. In that mode only subscribe/unsubscribe, `PING`,
and `QUIT` are accepted. `PUBLISH` sends `message` frames to channels and
`pmessage` frames to matching glob patterns. Every asynchronous write takes
the same per-client writer mutex as ordinary replies.

## Authentication

The default user starts enabled with `nopass` for local development. An admin
can configure a user with:

```text
ACL SETUSER app resetpass >correct-horse-battery-staple
AUTH app correct-horse-battery-staple
ACL WHOAMI
ACL GETUSER app
```

Passwords are stored as SHA-256 digests and compared with
`crypto/subtle.ConstantTimeCompare`. A user marked `off` or a failed password
gets `WRONGPASS`; unauthenticated connections receive `NOAUTH` for every
command except `AUTH` and `QUIT`.
