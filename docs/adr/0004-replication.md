# ADR 0004: canonical RESP frames for primary/replica replication

*Status: accepted*

## Context

The replication stages require handshake ordering, a full sync payload,
propagation, ACKs, and WAIT. Reusing the command encoder avoids a second wire
format and makes collection mutations replicate with the same argument rules.

## Decision

The primary sends `FULLRESYNC`, an RDB bulk snapshot, and then canonical RESP
command arrays. It tracks a byte offset per stream, sends `GETACK` for WAIT,
and removes replicas whose writes fail. Replicas apply incoming commands with
recording disabled and retry the upstream connection after a short delay.

## Consequences

The protocol is inspectable with the existing RESP reader and has no echo loop.
The implementation always full-syncs; a bounded backlog and `CONTINUE` path are
deferred until reconnect performance requires them. Replication links should
remain private until TLS/ACL support is added for that channel.
