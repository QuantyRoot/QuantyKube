# The image `kubectl apply` runs on a node (ADR-009, ADR-037). The
# binary is built outside, by CI on Ubuntu 24.04, whose glibc is older
# than this base's and than the nodes', so the same file runs in the
# container and, copied by `install store`, on the host.
FROM gcr.io/distroless/cc-debian13
COPY target/release/quantykube /usr/local/bin/quantykube
ENTRYPOINT ["/usr/local/bin/quantykube"]
