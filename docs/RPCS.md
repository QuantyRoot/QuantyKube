# What the API server calls

The acceptance list for the protocol side. It names every gRPC method
QuantyKube must answer, and in what shapes, so that "the etcd subset the
API server uses" (ADR-001) is a list that can be checked rather than a
phrase.

## What was built, as of 21 October

This file was written before the code and is kept as the plan it was.
The code went further than the plan where the API server or `etcdctl`
needed it, and differs from it in these points:

- Range: `limit` on a single key, `sort_order`, `sort_target`,
  `serializable` and the `min_` and `max_` filters are answered as etcd
  answers them, not rejected.
- Put: `ignore_value` and `ignore_lease` work. DeleteRange works outside
  a transaction.
- Txn: any shape is accepted, with compares on version, create, mod,
  value and lease, compares over a key range, and nested transactions,
  not only the five shapes below.
- Several writes share one commit (ADR-012); each write that changes
  something still gets one revision of its own.
- Watch: client chosen ids are accepted; ids the server assigns count up
  per stream and are never reused.
- Lease: LeaseGrant, LeaseRevoke, LeaseKeepAlive and LeaseTimeToLive.
  LeaseLeases is not supported.
- Maintenance: Status and Alarm (no alarms are ever raised). RangeStream
  is not built.

Known differences from etcd that the API server does not reach: a
transaction may name one key twice, an empty key is not refused, and
there is no limit on the operations in one transaction.

## Where this list comes from

From kine, which has carried production Kubernetes clusters for years
while implementing exactly this subset and rejecting the rest. The
reference is kine at commit `c5b90fc2846637765bb2bc6d92a60794f3ec8d42`
(16 October 2026), files under `pkg/server/`.

This is a starting point, not the final word. Kine's list is what kine
needed, and kine may answer some things the API server never asks. The
real check is the API server itself: once the transport works, every
request it sends is logged, and anything not on this list is either added
with a reason or shown never to happen.

## Services

Six gRPC services are registered: `etcdserverpb.KV`, `Watch`, `Lease`,
`Cluster`, `Maintenance`, and the standard `grpc.health.v1.Health`. The
API server's client is built for all of them, so each must exist even
where most of its methods only answer "not supported".

"Not supported" means a gRPC error with a clear message, never a hang and
never a crash.

## KV

**Range** is the main read. Two forms:

- A single key: `range_end` empty. Must support `revision` (read as of a
  past revision, which is the time travel QuantyDB gives for free),
  `keys_only` and `count_only`. `limit` on a single key is rejected.
- A key range: `range_end` set, used for every list in Kubernetes. Must
  support `revision`, `limit`, `keys_only`, `count_only`, and report
  `more` and `count`, which the API server uses for paging with
  continue tokens.
- Rejected as unsupported: `sort_order`, `sort_target`, `serializable`,
  and the `min_` and `max_` filters on create and mod revisions.

**RangeStream** returns a large range as a stream of batches. Recent
Kubernetes releases use it for big lists. Kine implements it; whether the
pinned API server calls it is to be confirmed in the request log, and it
is not needed for the first demonstration if it does not.

**Put** is used directly only for a few keys. `prev_kv` must be honored.
`ignore_value` and `ignore_lease` are rejected.

**DeleteRange** outside a transaction is not used and is rejected.

**Txn** is how the API server writes almost everything. Only these shapes
occur, and every other shape is rejected with an `InvalidArgument` error:

1. **Create.** One compare: `mod_revision` of the key `== 0`. Success: one
   Put. Failure: empty.
2. **Update.** One compare: `mod_revision` of the key `== N`. Success: one
   Put, possibly with a lease. Failure: one Range on the same key, so the
   caller gets the current value back on a conflict.
3. **Delete, guarded.** One compare: `mod_revision == N`. Success: one
   DeleteRange. Failure: one Range.
4. **Delete, unguarded.** No compare. Success: a Range followed by a
   DeleteRange on the same key.
5. **Compaction bookkeeping.** One compare: `version` of the key
   `compact_rev_key` equal to a number. Success: one Put to that key.
   Failure: one Range. The API server uses this to agree on who compacts.

Every one of these writes is one commit in QuantyDB, and so one revision.

**Compact** removes history before a revision. The API server calls it on
a timer. Here it becomes a retention boundary for QuantyDB's collector,
and how much history is kept on purpose beyond that is a later decision.

## Watch

One bidirectional stream per client, carrying many watches.

- Create requests with a key or key range and a `start_revision`.
  Client chosen watch ids are rejected; the server assigns ids.
- `prev_kv` on events.
- `progress_notify`, and progress requests sent on the stream. The API
  server's watch cache depends on these to know it is up to date.
- Cancel requests.
- A watch starting before the compacted revision is cancelled with the
  compaction revision, which tells the client to list again.
- Events arrive in revision order, and a reported revision never goes
  backwards on a stream.

Watch is the largest piece on this list and the one that needs streaming
over our own HTTP/2.

## Lease

- **LeaseGrant** only. The API server attaches leases to short lived
  objects such as events so they expire. Kine answers with the TTL as the
  lease id and expires keys by their TTL itself. Whether QuantyKube copies
  that shortcut or keeps real leases is decided when it is built.
- LeaseRevoke, LeaseKeepAlive, LeaseTimeToLive and LeaseLeases: not
  supported.

## Cluster

- **MemberList** returns one member, this server.
- MemberAdd, MemberRemove, MemberUpdate, MemberPromote: not supported.

## Maintenance

- **Status** returns the database size and an etcd version string. The
  version matters: the API server checks it to decide whether watch
  progress requests are available. Kine reports 3.6.11 by default.
- Alarm, Defragment, Hash, HashKV, Snapshot, MoveLeader, Downgrade: not
  supported. Snapshot is the first candidate to add later, because
  backup tools call it.

## Health

`grpc.health.v1.Health/Check` reports serving.

## How this list is used

Each item becomes a test that sends the exact request shape and checks the
answer, and each "not supported" becomes a test that checks the error.
Before 17 November, a run of the pinned API server is logged and compared
against this file.
