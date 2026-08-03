#!/bin/bash
#
# Mount PGDATA on LazyFS, prove the mount landed, then drop to postgres and
# exec pgbattery with whatever arguments the compose service passed.
#
# Every failure here is fatal and loud. A container that comes up with PGDATA
# on the ordinary filesystem instead of LazyFS is the worst outcome available:
# the durability suite would run green against a filesystem that cannot lose
# an un-fsynced write, and report that as evidence fsync is honoured.

set -euo pipefail

MOUNT_DIR="${PGBATTERY_LAZYFS_MOUNT:-/var/lib/postgresql/data}"
ROOT_DIR="${PGBATTERY_LAZYFS_ROOT:-/var/lib/postgresql/pgdata-root}"
CONFIG="${PGBATTERY_LAZYFS_CONFIG:-/etc/lazyfs.toml}"
FIFO="${PGBATTERY_LAZYFS_FIFO:-/tmp/lazyfs.fifo}"
FIFO_DONE="${PGBATTERY_LAZYFS_FIFO_COMPLETED:-/tmp/lazyfs.completed.fifo}"
MOUNT_TIMEOUT_S="${PGBATTERY_LAZYFS_MOUNT_TIMEOUT_S:-30}"

die() {
    echo "pgbattery-lazyfs: FATAL: $*" >&2
    exit 1
}

[ "$(id -u)" = "0" ] || die "must start as root to mount FUSE; got uid $(id -u)"
command -v lazyfs >/dev/null || die "lazyfs binary absent from the image"
[ -s "$CONFIG" ] || die "lazyfs config $CONFIG missing or empty"

# A previous incarnation of this container may have died with the mount still
# registered. Lazy-unmount it rather than failing, but only if it is genuinely
# a fuse mount — never unmount something we did not make.
if grep -qE " ${MOUNT_DIR} fuse" /proc/mounts 2>/dev/null; then
    echo "pgbattery-lazyfs: stale mount at ${MOUNT_DIR}, detaching"
    fusermount3 -uz "$MOUNT_DIR" || die "could not detach stale mount at ${MOUNT_DIR}"
fi

mkdir -p "$ROOT_DIR" "$MOUNT_DIR"

# PGDATA must be mode 0700 and owned by the running user or PostgreSQL refuses
# to start. With the subdir module the backing directory's ownership is what
# shows through the mount, so it is set on the root, not the mount point.
chown postgres:postgres "$ROOT_DIR" "$MOUNT_DIR"
chmod 0700 "$ROOT_DIR"

rm -f "$FIFO" "$FIFO_DONE"

echo "pgbattery-lazyfs: mounting ${MOUNT_DIR} on LazyFS backed by ${ROOT_DIR}"
# `-f` keeps FUSE in the foreground so the backgrounded PID is the filesystem
# itself. Without it FUSE daemonises, the launcher exits immediately, and the
# liveness check below would race a successful start.
lazyfs "$MOUNT_DIR" \
    --config-path "$CONFIG" \
    -f \
    -o allow_other \
    -o modules=subdir \
    -o subdir="$ROOT_DIR" &
LAZYFS_PID=$!

# Poll for the mount rather than sleeping at it. This is waiting on an
# external process to publish an observable fact, and the fact is checked;
# it is not a timer standing in for a state transition.
deadline=$((SECONDS + MOUNT_TIMEOUT_S))
while [ "$SECONDS" -lt "$deadline" ]; do
    if grep -qE " ${MOUNT_DIR} fuse" /proc/mounts 2>/dev/null; then
        break
    fi
    kill -0 "$LAZYFS_PID" 2>/dev/null || die "lazyfs exited before the mount appeared"
    sleep 0.2
done

grep -qE " ${MOUNT_DIR} fuse" /proc/mounts \
    || die "no fuse mount at ${MOUNT_DIR} after ${MOUNT_TIMEOUT_S}s"

# The control FIFO is how the suite discards un-fsynced writes. If it never
# appears, the crash injection would silently degrade into an ordinary
# SIGKILL, which this repo already covers and which proves nothing about fsync.
deadline=$((SECONDS + MOUNT_TIMEOUT_S))
while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -p "$FIFO" ]; then
        break
    fi
    sleep 0.2
done
[ -p "$FIFO" ] || die "lazyfs control fifo ${FIFO} never appeared"

# The harness reaches the FIFO as an unprivileged exec, so it has to be
# writable by more than root.
chmod 0666 "$FIFO"
[ -p "$FIFO_DONE" ] && chmod 0666 "$FIFO_DONE"

# Prove the mount is writable as postgres before handing it PGDATA. A mount
# that exists but rejects the writes is the same failure wearing a hat.
probe="${MOUNT_DIR}/.lazyfs-write-probe"
setpriv --reuid=postgres --regid=postgres --init-groups \
    -- sh -c "printf ok > '${probe}' && rm -f '${probe}'" \
    || die "postgres cannot write to the LazyFS mount at ${MOUNT_DIR}"

echo "pgbattery-lazyfs: mount verified, control fifo ${FIFO} ready"

mkdir -p /var/lib/postgresql/raft
chown postgres:postgres /var/lib/postgresql/raft

exec setpriv --reuid=postgres --regid=postgres --init-groups -- "$@"
