# syntax=docker/dockerfile:1.4
FROM rust:1.96 AS builder
WORKDIR /app

# Build profile: "release" (default) or "dev" for fast iteration
ARG BUILD_PROFILE=release

# pg_query crate requires libclang for bindgen
RUN apt-get update && apt-get install -y libclang-dev clang && rm -rf /var/lib/apt/lists/*

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo build --release && cp target/release/pgbattery /pgbattery; \
    else \
        cargo build && cp target/debug/pgbattery /pgbattery; \
    fi

FROM postgres:18
WORKDIR /app

RUN apt-get update && apt-get install -y tini libfaketime iproute2 procps curl && rm -rf /var/lib/apt/lists/*

# libfaketime installs at /usr/lib/<arch>-linux-gnu/faketime/libfaketime.so.1.
# On amd64 that's /usr/lib/x86_64-linux-gnu/...; on arm64 (Docker Desktop on
# Apple Silicon) it's /usr/lib/aarch64-linux-gnu/.... Hardcoding either in
# LD_PRELOAD silently fails on the other arch and every clock-skew chaos
# test silently no-ops. Symlink to a stable path and verify it resolves.
RUN ln -sf /usr/lib/*/faketime/libfaketime.so.1 /usr/local/lib/libfaketime.so.1 && \
    test -e /usr/local/lib/libfaketime.so.1

RUN echo "* soft nofile 65536" >> /etc/security/limits.conf && \
    echo "* hard nofile 65536" >> /etc/security/limits.conf

COPY --from=builder /pgbattery /usr/local/bin/pgbattery

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
