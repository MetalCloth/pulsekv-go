# ADR 0002: one synchronized store with versioned expiry

*Status: accepted*

## Context

Strings, lists, streams, sorted sets, bitmaps, and geospatial values must share
type checks, TTL behavior, and wake blocked readers. Independent maps without a
common lock invite concurrent map writes and stale expiry entries.

## Decision

Keep the maps behind `Store.mu`. Purge expiry under that lock, delete every
possible type map, increment a per-key version, and close a replaceable change
channel. Serialize mutations with `Server.mutationMu` so durability and
replication observe one order.

## Consequences

The model is easy to audit and makes WATCH and blocking reads deterministic. A
single mutation lock limits throughput; per-key locks are deferred until a
benchmark demonstrates the need and can preserve lock ordering.
