# Benchmarks against etcd

Measured with `scripts/bench.sh`, which builds etcd's own benchmark tool
from the etcd release it compares against and runs both stores on the
same machine, the same disk and the same kind of socket. Every number
here can be reproduced with it; numbers that cannot are not published
(ADR-002).

Run on 20 October 2026, QuantyKube at the group commit change.

## Machine

| | |
| --- | --- |
| CPU | 2 vCPU, Intel Xeon @ 2.10GHz, virtual machine |
| Memory | 8 GB |
| Disk | virtual block device, ext4, fdatasync 56 to 130 microseconds |
| etcd | v3.6.15, one member, default settings |
| Transport | Unix socket for both |
| Tool | etcd `tools/benchmark` built from v3.6.15 |

The benchmark client runs on the same two cores as the store, which
favors neither side but makes everything CPU bound sooner than on real
hardware. A slower disk would hurt whichever store syncs more often. The
real numbers come from the rented servers before 17 November.

## Results

Median of three runs, fresh store each run, 256 byte values.

| Workload | etcd req/s | QuantyKube req/s | etcd p99 ms | QuantyKube p99 ms |
| --- | --- | --- | --- | --- |
| put, 1 client | 1,392 | 982 | 2.09 | 2.54 |
| put, 16 clients | 7,858 | 4,421 | 6.61 | 7.11 |
| txn put, 16 clients | 7,463 | 4,528 | 6.64 | 7.56 |
| range one key, linearizable, 16 clients | 19,287 | 35,366 | 4.06 | 2.47 |
| range one key, serializable, 16 clients | 20,497 | 34,567 | 3.67 | 2.27 |
| watch event delivery, 100 watchers | | | 2.02 | 1.82 |

After the 16 client put run:

| | etcd | QuantyKube |
| --- | --- | --- |
| peak memory | 47.4 MiB | 20.8 MiB |
| data on disk | 130 MB | 169 MB |

With a k3s control plane on top, creating, listing and deleting 500
ConfigMaps, two runs each, measured with the build before group commit:

| | etcd | QuantyKube |
| --- | --- | --- |
| API server ready | 3.4 s | 3.3 to 3.4 s |
| store memory, idle | 44 MiB | 15 MiB |
| apply 500 ConfigMaps | 2.5 to 2.7 s | 2.6 to 3.0 s |
| list them | 0.14 to 0.19 s | 0.17 to 0.18 s |
| data on disk after | 124 MB | 45 MB |

## What this says

**Writes are slower.** A single client gets about 70% of etcd's rate:
QuantyDB commits with two fsyncs where etcd needs about one, and a lone
writer has nobody to share them with. Sixteen clients get about 56% of
etcd's rate. Before group commit (ADR-012) that was 20%; the store is now
bound by CPU per write, mostly QuantyDB's commit work, and that is the
next thing to profile.

**Reads are faster,** by about 1.8 times for a single key. A read here is
a lookup in a copy on write snapshot with no lock held against writers.
This workload reads one tiny key, so it measures the request path, not
storage at scale.

**Memory is a third to a half of etcd's,** in every run.

**Disk is a trade.** Each commit writes new pages for everything it
touched and keeps the old ones for the history window, an hour by
default. Under the put benchmark, which writes 20,000 new keys as fast as
it can, that is 169 MB against etcd's 130. Under the Kubernetes workload
it is 45 MB against 124. Space is reused once history ages out of the
window; the file does not shrink, and `etcdctl defrag` is not supported.

**In a Kubernetes control plane the stores are even.** The API server
takes the same time to become ready and to apply and list objects on
both. Deleting 500 objects took 100 seconds on both, which is a limit of
`kubectl delete`, not of either store.

## Earlier anomalies, explained

The first run of these benchmarks once saw k3s take 128 seconds to become
ready on QuantyKube, with k3s logging "certificate signed by unknown
authority" for its own port 6443. The same message appeared later when
two k3s instances ran on one machine at once: the new one was talking to
the old one, which still held the port. It is a property of the test
setup, not of the store, and it did not recur once runs were kept apart.

Before group commit, the 16 client put workload ran at 564 to 1,494
requests a second and the data file grew by up to 25 KB per write. Both
improved with ADR-012: 4,399 to 4,515 requests a second, and about 8.5 KB
per write.
