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

## Through the Kubernetes API

With `quantykube serve --api-listen`, the same three methods are an
aggregated API that kube-apiserver forwards to (ADR-028). The messages
are the ones above; a read carries its request base64url encoded:

| Request | Method |
| --- | --- |
| `GET /apis/quanty.quantyroot.io/v1alpha1/logs?q=<request>` | Log |
| `GET /apis/quanty.quantyroot.io/v1alpha1/objects?q=<request>` | Get |
| `POST /apis/quanty.quantyroot.io/v1alpha1/restores`, the request as body | Restore |

kube-apiserver authenticates the caller and checks RBAC before it
forwards, so a role grants `get` on `logs` and `objects` and `create`
on `restores` in the group `quanty.quantyroot.io`. The store takes only
connections with kube-apiserver's front proxy certificate
(`--api-client-ca`), and serves under the name of the Service it is
registered as (`--api-cert`). Registering it takes three objects: a
Service without a selector, an EndpointSlice with the store's address,
and an APIService with the CA of `--api-cert` as its `caBundle`;
`scripts/e2e.sh` has all three.

`kubectl-quanty` (`crates/kubectl-quanty`) makes these requests through
`kubectl get --raw` and `kubectl create --raw`, so it needs only the
kubeconfig kubectl uses, and is built for Windows in CI.
