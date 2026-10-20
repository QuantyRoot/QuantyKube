# The history service

The etcd API forgets what the API server compacts. QuantyKube keeps it for
`--retain` (ADR-011) and serves it on the same socket as a gRPC service of
its own, `quantykube.History`. The command line uses it; other tools can
too. Messages are protobuf.

## Request, the same for every method

| Field | Number | Type | Meaning |
| --- | --- | --- | --- |
| key | 1 | bytes | the key, or the start of a range |
| range_end | 2 | bytes | end of the range, exclusive; `Log` only |
| revision | 3 | int64 | the revision to read |
| unix_ms | 4 | int64 | instead of `revision`: the state at this time |

## Log

All retained changes to `key`, or to `[key, range_end)`, oldest first.
The response repeats field 1, one entry per change:

| Field | Number | Type |
| --- | --- | --- |
| revision | 1 | int64 |
| unix_ms | 2 | int64 |
| deleted | 3 | int64, 1 for a delete |
| kv | 4 | mvccpb.KeyValue, the new state |
| prev_kv | 5 | mvccpb.KeyValue, the state before, absent on create |

## Get

The key as it was. The response has the revision read in field 1 and the
`mvccpb.KeyValue` in field 2, absent if the key did not exist then.

## Restore

Writes the key back as it was: its value if it existed, a delete if it
did not. This is an ordinary write with a new revision, so every watch
sees it and the API server's cache follows. The response is that of
`Get`, plus the new revision in field 3. Leases are not restored; a key
that had one comes back without it.

A revision older than the retention window answers OUT_OF_RANGE.
