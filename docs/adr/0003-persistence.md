# ADR 0003: AOF for commands, RDB for compact string snapshots

*Status: accepted*

## Context

The CodeCrafters progression exercises both AOF replay and RDB parsing. A full
Redis serializer would distract from command semantics while still needing
crash-safe log handling.

## Decision

Write successful mutations as RESP arrays to a manifest-backed incremental AOF.
Replay through the normal dispatcher with recording disabled. Generate and read
a minimal Redis-compatible RDB containing string values and expiries for SAVE
and PSYNC.

## Consequences

Recovery reuses command behavior and preserves collection writes in the AOF.
RDB load is useful for the string stages and a compact sync payload. AOF
rewrite/compaction and collection RDB opcodes are explicit future work rather
than hidden partial implementations.
