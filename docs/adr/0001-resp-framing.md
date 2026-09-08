# ADR 0001: length-aware RESP2 framing

*Status: accepted*

## Context

Redis values can contain arbitrary bytes, including CR and LF, and clients can
pipeline requests. A line-oriented parser appears short but cannot tell a
payload newline from a protocol newline.

## Decision

Use a bounded recursive RESP2 reader. Bulk strings are allocated from their
declared length with `io.ReadFull`, followed by an exact CRLF check. Arrays are
bounded by item count and nesting depth.

## Consequences

The parser is binary-safe and keeps the next pipelined frame intact. It has a
small amount of explicit length/error code and rejects malformed input early.
RESP3 is outside this repository's current acceptance surface.
