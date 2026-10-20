# QuantyKube

A Kubernetes data store with time travel, written in Rust, depending only
on the standard library and other QuantyRoot crates.

[![CI](https://github.com/QuantyRoot/QuantyKube/actions/workflows/ci.yml/badge.svg)](https://github.com/QuantyRoot/QuantyKube/actions/workflows/ci.yml)
![Status](https://img.shields.io/badge/status-early-red)
[![Made by Elchi](https://img.shields.io/badge/made%20by-Elchi-8A2BE2)](https://github.com/Elchi-dev)

## Read this first

QuantyKube is not a general etcd replacement. It implements only the part
of the etcd v3 API that the Kubernetes API server uses, on a single node,
without clustering. Other etcd clients may call things that do not exist
here. This is a deliberate choice, recorded in
[ADR-001](docs/DECISIONS.md), and the same one kine makes.

Nothing is built yet. Everything below is the plan.

## The idea

etcd stores every object in a Kubernetes cluster as a key with a global,
ever increasing revision. QuantyKube puts that API on top of the QuantyDB
engine, which already stores data as numbered, immutable commits. The
revision is the commit. Reading the cluster as it was two hours ago is not
an added feature; it is how the engine reads anything.

## Plan

| Part | State |
| --- | --- |
| etcd v3 subset used by kube-apiserver, Unix socket | planned |
| Watch | planned |
| Reading the cluster at a past revision | planned |
| TLS (via QuantyCore) | later |
| Node runtime behind CRI ([ADR-005](docs/DECISIONS.md)) | later |
| Clustering | not planned for now |

What the first demonstration has to do is in
[docs/ACCEPTANCE.md](docs/ACCEPTANCE.md).

## Building

```
cargo test
```

Rust 1.89 or newer.
