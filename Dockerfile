# syntax=docker/dockerfile:1.7
FROM rust:1.97 AS builder
WORKDIR /app

# Cargo profile for the binary. `ci` is release optimisation without the
# whole-program LTO that costs two minutes a link and buys tests nothing; see
# [profile.ci] in Cargo.toml.
#
# Safe as a default only because nothing published comes from this file:
# `release.yml` runs `cargo build --release` against the target triple directly,
# and ci.yml's Docker job is `push: false`. Anything that starts publishing an
# image built here must pass `--build-arg BUILD_PROFILE=release`.
ARG BUILD_PROFILE=ci

# pg_query crate requires libclang for bindgen
RUN apt-get update && apt-get install -y libclang-dev clang && rm -rf /var/lib/apt/lists/*

COPY . .

# Strip build-host paths from the binary: panic locations, tracing callsite
# metadata, and file!() strings from registry deps embed absolute paths as
# data, which `strip` cannot remove.
ENV RUSTFLAGS="--remap-path-prefix=/app=/src --remap-path-prefix=/usr/local/cargo=/cargo"

# Cargo writes a custom profile to target/<profile>/; only `dev` is spelled
# differently. Naming the profile rather than branching on it means a new one
# needs no change here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --profile "$BUILD_PROFILE" --locked && \
    out=$([ "$BUILD_PROFILE" = "dev" ] && echo debug || echo "$BUILD_PROFILE") && \
    cp "target/$out/pgbattery" /pgbattery

FROM postgres:18 AS runtime-base
WORKDIR /app

RUN apt-get update && apt-get install -y tini libfaketime iproute2 iptables procps curl && rm -rf /var/lib/apt/lists/*

# Chaos faults need these in-container: iproute2 for `tc netem` (latency, loss),
# iptables for asymmetric partitions (one-directional DROP). Absent, the fault
# step exits 127 and the case still passes — a partition test that partitions
# nothing. Verified at build time for the same reason as libfaketime below.
RUN command -v tc && command -v iptables

# libfaketime installs at /usr/lib/<arch>-linux-gnu/faketime/libfaketime.so.1.
# On amd64 that's /usr/lib/x86_64-linux-gnu/...; on arm64 (Docker Desktop on
# Apple Silicon) it's /usr/lib/aarch64-linux-gnu/.... Hardcoding either in
# LD_PRELOAD silently fails on the other arch and every clock-skew chaos
# test silently no-ops. Symlink to a stable path and verify it resolves.
RUN ln -sf /usr/lib/*/faketime/libfaketime.so.1 /usr/local/lib/libfaketime.so.1 && \
    test -e /usr/local/lib/libfaketime.so.1

RUN echo "* soft nofile 65536" >> /etc/security/limits.conf && \
    echo "* hard nofile 65536" >> /etc/security/limits.conf

RUN mkdir -p /var/lib/postgresql/data /var/lib/postgresql/raft && \
    chown -R postgres:postgres /var/lib/postgresql

USER postgres

# Probe the management API's leader-discovery endpoint. It is unauthenticated
# (per the discovery contract) and returns HTTP 200 with a JSON body iff the
# node has processed at least one cluster-state update. A node whose Raft
# loop is wedged but whose PG process is alive will still fail this check —
# exactly the case docker-compose / Kubernetes need to act on. We use
# `start-period=60s` because cold-start bootstrap (initdb + pg_basebackup
# rejoin in worst case) can briefly exceed the steady-state response time.
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -sf --max-time 4 http://127.0.0.1:9091/api/v1/cluster/leader > /dev/null || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["pgbattery", "run"]

# The binary is the only thing separating these two stages, and it is the last
# layer, so both reuse every layer above and switching between them is a copy.
FROM runtime-base AS runtime
COPY --from=builder /pgbattery /usr/local/bin/pgbattery

# Local iteration only (`scripts/dev-image.sh`), never CI. Every `COPY . .` in
# the builder hands cargo fresh mtimes, so the in-VM build refingerprints and
# rebuilds all three workspace crates for a one-line edit; a host cross-build
# keeps its own fingerprints and does the same work in a third of the time.
FROM runtime-base AS runtime-prebuilt
COPY .devbin/pgbattery /usr/local/bin/pgbattery

# ─────────────────────────────────────────────────────────────────────────────
# LazyFS build — durability testing only, never in the default image.
#
# `docker-compose.lazyfs.yml` targets the `runtime-lazyfs` stage below; every
# other compose file targets `runtime` and pays none of this build cost.
#
# LazyFS is a FUSE filesystem that holds un-fsynced writes in its own userspace
# page cache and can be told to discard exactly those. That is the only way to
# observe lost-unsynced-writes here. The scaffold this replaces proposed
# libeatmydata, which cannot do it: making fsync() a no-op still leaves the
# write in the *host* page cache, and SIGKILLing a container does not discard
# host page cache, so the data is all still there on restart. A durability
# assertion built on libeatmydata would pass while proving nothing.
#
# Built from the same postgres:18 base as the runtime stage so libstdc++ and
# glibc match exactly, and pinned to a release commit SHA rather than a tag —
# tags move (see tla/Makefile for what that costs).
# ─────────────────────────────────────────────────────────────────────────────
FROM postgres:18 AS lazyfs-builder

ARG LAZYFS_COMMIT=045a0b3a1126725e693934e29d3ba15e08cc39ec

RUN apt-get update && apt-get install -y \
        g++ cmake make git ca-certificates libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/dsrhaslab/lazyfs.git /build/lazyfs \
    && git -C /build/lazyfs checkout --detach "${LAZYFS_COMMIT}" \
    && test "$(git -C /build/lazyfs rev-parse HEAD)" = "${LAZYFS_COMMIT}"

RUN cd /build/lazyfs/libs/libpcache && ./build.sh
RUN cd /build/lazyfs/lazyfs && ./build.sh

# The build scripts emit into build/ without an install step, so the binary
# path is asserted here rather than assumed by a later COPY that would happily
# copy nothing.
RUN test -x /build/lazyfs/lazyfs/build/lazyfs

FROM runtime AS runtime-lazyfs

USER root

# `fuse3` brings fusermount3 and pulls the matching runtime libfuse3 as a
# dependency. The library package is deliberately not named here: its soname
# suffix tracks the Debian release under postgres:18 and pinning the wrong one
# fails the build on a base image bump. The ldd check below is what actually
# guarantees the right library arrived.
RUN apt-get update && apt-get install -y fuse3 && rm -rf /var/lib/apt/lists/*

# Neither build.sh has an install step, so both artifacts are copied out of
# the build tree. lazyfs links libpcache through an rpath into that tree, which
# does not exist here; the loader falls through to the ldconfig cache, so
# libpcache has to be in a standard path and ldconfig has to have run.
COPY --from=lazyfs-builder /build/lazyfs/lazyfs/build/lazyfs /usr/local/bin/lazyfs
COPY --from=lazyfs-builder /build/lazyfs/libs/libpcache/build/libpcache.so /usr/local/lib/libpcache.so
COPY --from=lazyfs-builder /build/lazyfs/libs/libpcache/build/libpcache.so.0 /usr/local/lib/libpcache.so.0

RUN ldconfig

# postgres must reach a mount made by root, which FUSE forbids unless
# `user_allow_other` is enabled and the mount passes `-o allow_other`. Without
# both, the mount succeeds and PGDATA is silently inaccessible to the process
# that needs it.
RUN echo "user_allow_other" >> /etc/fuse.conf

COPY testing/lazyfs/entrypoint.sh /usr/local/bin/pgbattery-lazyfs
# Two configs for two LazyFS instances: PGDATA and the Raft store. They must
# differ in fifo_path and logfile, or the second instance would create a FIFO
# over the first's and faults would reach whichever won the race.
COPY testing/lazyfs/lazyfs.toml /etc/lazyfs.toml
COPY testing/lazyfs/lazyfs-raft.toml /etc/lazyfs-raft.toml

RUN chmod 0755 /usr/local/bin/pgbattery-lazyfs

# Same reason as `command -v tc` above: a binary that cannot load turns every
# durability assertion into a test of nothing. `test -x` is not enough — the
# failure mode here is an unresolved libpcache, which leaves the file present
# and executable and failing only at run time, inside a container whose logs
# nobody reads until the suite has already reported green.
RUN ldd /usr/local/bin/lazyfs && ! ldd /usr/local/bin/lazyfs | grep -q "not found"
RUN test -x /usr/local/bin/pgbattery-lazyfs && test -s /etc/lazyfs.toml && test -s /etc/lazyfs-raft.toml
# The two instances must not share a control FIFO or a log. If they did, one
# would clobber the other's and faults aimed at PGDATA would land on the Raft
# store or vanish, which no assertion downstream could distinguish from a
# fault that simply did nothing.
RUN test "$(grep -c '/tmp/lazyfs-raft' /etc/lazyfs-raft.toml)" -ge 2 \
    && ! grep -qE 'fifo_path *= *"/tmp/lazyfs\.fifo"' /etc/lazyfs-raft.toml \
    && ! grep -qE 'logfile *= *"/tmp/lazyfs\.log"' /etc/lazyfs-raft.toml
RUN command -v setpriv && command -v fusermount3

# Stays root: the entrypoint mounts FUSE and then drops to postgres itself.
# PostgreSQL refuses to run as root, so the drop is not optional.
USER root
