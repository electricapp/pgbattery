#!/bin/bash
#
# Mount PGDATA and the Raft store on LazyFS, prove both mounts landed, then
# drop to postgres and exec pgbattery with whatever arguments the compose
# service passed.
#
# Every failure here is fatal and loud. A container that comes up with either
# directory on the ordinary filesystem is the worst outcome available: the
# durability suite would run green against a filesystem that cannot lose an
# un-fsynced write, and report that as evidence fsync is honoured.
#
# Two LazyFS instances rather than one covering their common parent. PGDATA
# needs mode 0700 and the Raft store does not; more importantly, a single
# instance would mean a fault aimed at one store crashes the filesystem holding
# the other, and every suite would lose the ability to say which store the
# damage was aimed at.

set -euo pipefail

MOUNT_TIMEOUT_S="${PGBATTERY_LAZYFS_MOUNT_TIMEOUT_S:-30}"

PG_MOUNT_DIR="${PGBATTERY_LAZYFS_MOUNT:-/var/lib/postgresql/data}"
PG_ROOT_DIR="${PGBATTERY_LAZYFS_ROOT:-/var/lib/postgresql/pgdata-root}"
PG_CONFIG="${PGBATTERY_LAZYFS_CONFIG:-/etc/lazyfs.toml}"
PG_FIFO="${PGBATTERY_LAZYFS_FIFO:-/tmp/lazyfs.fifo}"
# Matches `[filesystem].logfile` in lazyfs.toml. The fault worker announces
# itself here, which is the only evidence that a command written to the FIFO
# will actually be read.
PG_LOGFILE="${PGBATTERY_LAZYFS_LOG:-/tmp/lazyfs.log}"

RAFT_MOUNT_DIR="${PGBATTERY_LAZYFS_RAFT_MOUNT:-/var/lib/postgresql/raft}"
RAFT_ROOT_DIR="${PGBATTERY_LAZYFS_RAFT_ROOT:-/var/lib/postgresql/raft-root}"
RAFT_CONFIG="${PGBATTERY_LAZYFS_RAFT_CONFIG:-/etc/lazyfs-raft.toml}"
RAFT_FIFO="${PGBATTERY_LAZYFS_RAFT_FIFO:-/tmp/lazyfs-raft.fifo}"
RAFT_LOGFILE="${PGBATTERY_LAZYFS_RAFT_LOG:-/tmp/lazyfs-raft.log}"

die() {
    echo "pgbattery-lazyfs: FATAL: $*" >&2
    exit 1
}

# Mount one LazyFS instance and refuse to return until it is demonstrably
# usable: mounted, with a control FIFO, with a fault worker reading it, and
# writable by postgres. Each check exists because the failure it catches would
# otherwise present as a passing test.
#
#   $1 mount dir  $2 backing root  $3 config  $4 fifo  $5 logfile  $6 mode
mount_lazyfs() {
    local mount_dir="$1" root_dir="$2" config="$3" fifo="$4" logfile="$5" mode="$6"
    local pid deadline

    [ -s "$config" ] || die "lazyfs config $config missing or empty"

    # A previous incarnation of this container may have died with the mount
    # still registered. Lazy-unmount it rather than failing, but only if it is
    # genuinely a fuse mount -- never unmount something we did not make.
    if grep -qE " ${mount_dir} fuse" /proc/mounts 2>/dev/null; then
        echo "pgbattery-lazyfs: stale mount at ${mount_dir}, detaching"
        fusermount3 -uz "$mount_dir" || die "could not detach stale mount at ${mount_dir}"
    fi

    mkdir -p "$root_dir" "$mount_dir"

    # With the subdir module the backing directory's ownership and mode are
    # what show through the mount, so they are set on the root, not the mount
    # point. PGDATA must be 0700 or PostgreSQL refuses to start.
    chown postgres:postgres "$root_dir" "$mount_dir"
    chmod "$mode" "$root_dir"

    # The log is removed, not merely truncated by LazyFS at startup: this
    # container may be a restart, and a previous incarnation's "waiting for
    # fault commands" line would satisfy the check below before LazyFS had
    # truncated the file.
    rm -f "$fifo" "$logfile"

    echo "pgbattery-lazyfs: mounting ${mount_dir} on LazyFS backed by ${root_dir}"
    # `-f` keeps FUSE in the foreground so the backgrounded PID is the
    # filesystem itself. Without it FUSE daemonises, the launcher exits
    # immediately, and the liveness checks below would race a successful start.
    lazyfs "$mount_dir" \
        --config-path "$config" \
        -f \
        -o allow_other \
        -o modules=subdir \
        -o subdir="$root_dir" &
    pid=$!

    # Poll for the mount rather than sleeping at it. This waits on an external
    # process to publish an observable fact, and the fact is checked; it is not
    # a timer standing in for a state transition.
    deadline=$((SECONDS + MOUNT_TIMEOUT_S))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -qE " ${mount_dir} fuse" /proc/mounts 2>/dev/null && break
        kill -0 "$pid" 2>/dev/null || die "lazyfs exited before ${mount_dir} appeared"
        sleep 0.2
    done
    grep -qE " ${mount_dir} fuse" /proc/mounts \
        || die "no fuse mount at ${mount_dir} after ${MOUNT_TIMEOUT_S}s"

    # The control FIFO is how faults reach this filesystem. If it never
    # appears, injection would silently degrade into an ordinary SIGKILL,
    # which this repo already covers and which proves nothing about fsync.
    deadline=$((SECONDS + MOUNT_TIMEOUT_S))
    while [ "$SECONDS" -lt "$deadline" ]; do
        [ -p "$fifo" ] && break
        sleep 0.2
    done
    [ -p "$fifo" ] || die "lazyfs control fifo ${fifo} never appeared"

    # The harness reaches the FIFO as an unprivileged exec, so it has to be
    # writable by more than root.
    chmod 0666 "$fifo"

    # The FIFO existing does not mean anything is reading it. LazyFS creates
    # it, opens it O_RDWR, and only then enters the loop that consumes
    # commands; if it stalls before that, writes to the FIFO still succeed and
    # every fault sits unread in the pipe buffer. The worker announces the
    # loop, so wait for the announcement rather than for the FIFO.
    deadline=$((SECONDS + MOUNT_TIMEOUT_S))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -q "waiting for fault commands" "$logfile" 2>/dev/null && break
        kill -0 "$pid" 2>/dev/null || die "lazyfs exited before its fault worker started"
        sleep 0.2
    done
    grep -q "waiting for fault commands" "$logfile" 2>/dev/null \
        || die "lazyfs fault worker for ${mount_dir} never started consuming commands; faults would inject nothing"

    chmod 0644 "$logfile"

    # Prove the mount is writable as postgres before anything is handed to it.
    # A mount that exists but rejects writes is the same failure wearing a hat.
    local probe="${mount_dir}/.lazyfs-write-probe"
    setpriv --reuid=postgres --regid=postgres --init-groups \
        -- sh -c "printf ok > '${probe}' && rm -f '${probe}'" \
        || die "postgres cannot write to the LazyFS mount at ${mount_dir}"

    echo "pgbattery-lazyfs: ${mount_dir} verified, control fifo ${fifo} ready"
}

[ "$(id -u)" = "0" ] || die "must start as root to mount FUSE; got uid $(id -u)"
command -v lazyfs >/dev/null || die "lazyfs binary absent from the image"

mount_lazyfs "$PG_MOUNT_DIR" "$PG_ROOT_DIR" "$PG_CONFIG" "$PG_FIFO" "$PG_LOGFILE" 0700

# The Raft store. pgbattery derives this directory as a sibling of pg_data_dir
# and keeps `raft.db` there, so without this mount redb's writes land on an
# ordinary filesystem and no fault can reach them -- which is exactly why the
# dirty-crash suite could say nothing about Raft durability.
mount_lazyfs "$RAFT_MOUNT_DIR" "$RAFT_ROOT_DIR" "$RAFT_CONFIG" "$RAFT_FIFO" "$RAFT_LOGFILE" 0750

exec setpriv --reuid=postgres --regid=postgres --init-groups -- "$@"
