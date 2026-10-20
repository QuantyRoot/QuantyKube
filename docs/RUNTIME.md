# The node runtime: design

ADR-005 says what "sandbox" means here: the program the kubelet drives
through the Container Runtime Interface on every node, which is
containerd or CRI-O in most clusters today. This file is the design,
written before most of the code.

## What exists

- `quantykube unpack` applies real image layers to a directory, safely
  (ADR-016).
- `quantykube sandbox` starts a command the way runc's default
  configuration does: new mount, PID, UTS, IPC and network namespaces;
  the root pivoted, read only if asked; a fresh `/dev` with the six
  standard devices, `devpts` and `/dev/shm`; `/sys` read only; the paths
  runc masks or makes read only under `/proc` and `/sys`; Docker's
  fourteen default capabilities in every set, the rest dropped from the
  bounding set; `no_new_privs`; a seccomp filter; and, with `--cgroup`, a
  cgroup v2 with memory and pid limits that the process is in before any
  of its own code runs (ADR-013). alpine, busybox and nginx run in it.

- `quantykube runtime` serves RuntimeService and ImageService on a Unix
  socket: version and status, pods and containers from creation to
  removal, their logs in the format the kubelet reads, and images
  imported with `quantykube import` (ADR-017, ADR-018).
  `scripts/cri.sh` takes a pod through its life with `crictl`. What the
  kubelet needs beyond that, in order: volumes, CNI, the cgroup limits
  it sends, users other than root, ExecSync, pulling, stats.

`scripts/sandbox.sh` checks all of that from inside the container and
times a start against runc on the same root filesystem. On the
development machine, two runs of 50 starts of `/bin/true` each, quiet
machine, runc v1.3.2 with the configuration `runc spec` writes:

| | median | p90 |
| --- | --- | --- |
| `quantykube sandbox` | 3.6 to 3.7 ms | 5.0 to 5.1 ms |
| `runc run` | 20.4 to 21.0 ms | 28.5 to 30.2 ms |

What the gap is made of, as far as is known: runc is a Go binary that
starts, re-executes itself to set up namespaces in C, and talks to its
child over a socket; the sandbox clones once. Two differences remain on
the sandbox's side and are why this is not yet a claim. Its seccomp
filter is a denylist of the 47 calls Docker refuses that lead out of a
container or into the kernel, where Docker's profile is an allowlist of
about 350 and so also refuses what nobody has thought of yet. And runc
applies AppArmor or SELinux labels where the host has them, which this
machine does not.

## What the kubelet calls

Taken from `pkg/apis/runtime/v1/api.proto` in kubernetes/cri-api. Two
gRPC services on a Unix socket, the same transport QuantyCore already
serves for the store.

**RuntimeService**, grouped by what must work first:

| Group | Methods |
| --- | --- |
| Lifecycle | Version, Status, RuntimeConfig, RunPodSandbox, StopPodSandbox, RemovePodSandbox, PodSandboxStatus, ListPodSandbox, CreateContainer, StartContainer, StopContainer, RemoveContainer, ListContainers, ContainerStatus |
| Kubelet needs these to stay healthy | ContainerStats, ListContainerStats, PodSandboxStats, ListPodSandboxStats, ReopenContainerLog, UpdateContainerResources, GetContainerEvents |
| `kubectl exec`, `attach`, `port-forward` | ExecSync, Exec, Attach, PortForward |
| Later or never | the Stream* variants, CheckpointContainer, CheckpointPod, RestorePod, ListMetricDescriptors, ListPodSandboxMetrics, UpdateRuntimeConfig, UpdatePodSandboxResources |

**ImageService:** ListImages, ImageStatus, PullImage, RemoveImage,
ImageFsInfo.

Exec, Attach and PortForward do not stream over gRPC. They return a URL,
and the kubelet proxies the client's connection to an HTTP server the
runtime runs. That server is a piece of its own.

## What a pod is here

containerd starts a "pause" container in every pod, a process whose only
job is to hold the pod's namespaces open, and puts the pod's containers
into them. This runtime holds the namespaces without a process: the
network, IPC and UTS namespaces are kept alive by bind mounting their
`/proc/<pid>/ns/*` files, and a container joins them with `setns`.

This is not new. CRI-O can drop its infra container in the same way, as
long as the pod does not share a PID namespace. It is still the first
measurable difference against containerd: one process and one container
start fewer per pod. A pod that asks for a shared PID namespace gets a
small holder process, because a PID namespace dies with its first
process.

## A container start, step by step

1. Resolve the image to an unpacked root filesystem on the node:
   overlayfs with the image's layers as lower directories and a fresh
   upper directory per container.
2. `clone3` with new mount and PID namespaces (and a user namespace when
   the pod asks for one), joining the pod's network, IPC and UTS
   namespaces.
3. In the child: mount `/proc`, `/dev`, `/sys` and the pod's volumes,
   `pivot_root` into the root filesystem, apply capabilities, seccomp and
   no_new_privs, set uid and gid, `execve` the entrypoint.
4. In the runtime: place the child in its cgroup v2 directory with the
   CPU, memory and pid limits the kubelet sent, and wire its stdout and
   stderr to the CRI log file format the kubelet reads.

containerd does the same through runc, a second binary started per
container, plus a shim process per pod. Doing it in-process is the second
measurable difference.

## Images

The registry HTTP API v2: token authentication, image index and manifest
resolution for the node's platform, layer blobs checked by SHA-256 and
unpacked from gzip or zstd tar into a content addressed store. Every part
of this is a dependency to write: HTTPS (QuantyCore's TLS), gzip
inflate, zstd, tar. This is why the runtime comes after TLS.

## Networking

Pods get their network from the standard CNI plugins. They are separate
executables called with a JSON configuration and the path of the pod's
network namespace; the runtime runs them, it does not reimplement them.

## Unsafe code

Namespaces, mounts, cgroups and `clone3` are syscalls the standard
library does not wrap. They go behind one small crate with every unsafe
block in it, the way QuantyDB's `quantydb-sys` holds its syscalls, and
nothing above that boundary is unsafe.

## How it is judged

Against containerd with runc on the same node, the kubelet unchanged:

- pod start latency, from RunPodSandbox to the container running, for
  a pod with one container and for one with three;
- memory on the node per running pod, runtime and shims included;
- time to pull and unpack a common image, cold;
- the node conformance tests the kubelet project publishes.

The numbers for containerd are taken first, on the rented nodes, so the
talk can show where the time goes before any of this is built.
