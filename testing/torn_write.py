#!/usr/bin/env -S uv run --project testing python
"""Torn-write oracle (H-25).

PROVES: that a write torn in half by a power loss is either repaired or
detected, never silently accepted as valid data. A torn page keeps its length,
so nothing about the file's metadata gives it away; if PostgreSQL served the
mixed page as though it were whole, every layer above it would read consistent
data that never existed.

HOW: ``lazyfs::torn-op`` splits the next write to one file into two halves,
writes only the first straight to the backing store, and crashes LazyFS so the
second half dies in its userspace cache. A one-row table is the instrument: heap
tuples fill a page from the end downward, so both tuple versions sit in the
second half while the page header and line pointers sit in the first. Persisting
only the first half therefore leaves a NEW header over a STALE tuple area, which
is a genuinely torn page rather than a truncated one.

WHY THE INVERSION IS NOT OPTIONAL: ``full_page_writes`` is what repairs the
page -- it puts a full-page image in WAL at the first modification after a
checkpoint, and redo overwrites the page with it. A run where the tear silently
failed to inject looks exactly like a run where recovery worked. So
``--prove-oracle`` first runs the same tear with ``full_page_writes=off``, where
the damage must survive and PostgreSQL's data checksums must reject the page,
and refuses to report on the real assertion unless that went red.

DOES NOT PROVE: anything about redb. The Raft store lives outside the LazyFS
mount, so no tear here can reach it. Nor torn WAL records or a torn pg_control,
which have their own CRCs and are not exercised.

Run:
    COMPOSE_FILE=docker-compose.lazyfs.yml docker compose --profile tornwrite up -d --build pg
    COMPOSE_FILE=docker-compose.lazyfs.yml uv run --project testing \
        python testing/torn_write.py --prove-oracle
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from typing import Final, TypedDict

import typer

import fault_primitives as fp

SERVICE: Final[str] = "pg"
"""The bare-PostgreSQL service in docker-compose.lazyfs.yml, behind the
`tornwrite` profile so the durability cluster does not start it."""

MOUNT: Final[str] = fp.PG_DATA_DIR
ROOT: Final[str] = fp.LAZYFS_ROOT_DIR

OLD_MARKER: Final[str] = "MARKERALPHA"
NEW_MARKER: Final[str] = "MARKERBETA"
"""Distinct lengths would make `left(v, n)` comparisons ambiguous; distinct
prefixes would not survive a grep of the raw page. Both are 10-11 characters of
uppercase ASCII, which cannot occur by accident in a page header."""

WAL_TABLE: Final[str] = "torn_meta_probe"

PAGE_BYTES: Final[int] = 8192
HALF_PAGE_BYTES: Final[int] = PAGE_BYTES // 2

START_TIMEOUT_S: Final[float] = 60.0


class TornWriteViolation(RuntimeError):
    """A torn page was served as though it were whole."""


class OracleNotProven(RuntimeError):
    """The inversion did not go red, so a green result would mean nothing."""


class HeapTearJson(TypedDict):
    """Serialised heap-page tear cycle."""

    weakened_durability: bool
    torn_bytes: int | None
    tore: bool
    postgres_started: bool
    row_value: str | None
    checksum_error: bool
    repaired: bool
    detected: bool
    notes: list[str]
    contracts: list[str]


class MetadataTearJson(TypedDict):
    """Serialised WAL / control-file tear cycle. Narrower contract than a heap
    page: neither structure has a full-page image and neither can be rebuilt
    from elsewhere in the cluster."""

    target: str
    mangled: bool
    file: str
    torn_bytes: int | None
    acked_before: int
    surviving: int
    lost_acked: list[int]
    postgres_started: bool
    complaint: str
    detected: bool
    contracts: list[str]


@dataclass
class Outcome:
    """What one tear-and-recover cycle produced."""

    weakened: bool
    torn_bytes: int | None = None
    backing_has_old: bool = False
    backing_has_new: bool = False
    postgres_started: bool = False
    row_value: str | None = None
    checksum_error: bool = False
    notes: list[str] = field(default_factory=list)

    @property
    def tore(self) -> bool:
        """Whether the backing store really holds a half-written page.

        Both halves are checked, not just one. A page missing the new marker
        could equally be a page that was never written at all, and that would
        make every assertion below vacuous.
        """
        return (
            self.torn_bytes == HALF_PAGE_BYTES and self.backing_has_old and not self.backing_has_new
        )

    @property
    def repaired(self) -> bool:
        return self.postgres_started and self.row_value == NEW_MARKER

    @property
    def detected(self) -> bool:
        return self.checksum_error

    def as_json(self) -> HeapTearJson:
        return HeapTearJson(
            weakened_durability=self.weakened,
            torn_bytes=self.torn_bytes,
            tore=self.tore,
            postgres_started=self.postgres_started,
            row_value=self.row_value,
            checksum_error=self.checksum_error,
            repaired=self.repaired,
            detected=self.detected,
            notes=self.notes,
            contracts=["R2"],
        )


def psql(sql: str, *, expect_ok: bool = True) -> str:
    """Run one statement as the postgres user and return its output."""
    result = fp.exec_in(
        SERVICE,
        f"setpriv --reuid=postgres --regid=postgres --init-groups -- "
        f'psql -U postgres -At -q -c "{sql}"',
    )
    if expect_ok and not result.ok:
        raise fp.FaultPreconditionError(f"psql failed for {sql!r}: {result.output}")
    return result.stdout.strip()


def pg_ctl(action: str, *, expect_ok: bool = True) -> fp.CommandResult:
    result = fp.exec_in(
        SERVICE,
        f"setpriv --reuid=postgres --regid=postgres --init-groups -- "
        f"pg_ctl -D {MOUNT} -l /tmp/pg.log {action}",
        timeout_s=START_TIMEOUT_S,
    )
    if expect_ok and not result.ok:
        raise fp.FaultPreconditionError(f"pg_ctl {action} failed: {result.output}")
    return result


def reset_cluster() -> None:
    """Reinitialise PGDATA so each cycle starts from the same place.

    Each cycle needs its own initdb: the weakened cycle deliberately leaves a
    corrupt page behind, and reusing it would let one cycle's damage decide the
    next cycle's verdict.
    """
    fp.exec_in(SERVICE, "pkill -9 -u postgres postgres || true")
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if not fp.exec_in(SERVICE, "pgrep -u postgres postgres || true").stdout.strip():
            break
        time.sleep(0.2)

    wiped = fp.exec_in(SERVICE, f"rm -rf {MOUNT:s}/* {MOUNT:s}/.[!.]* 2>/dev/null || true")
    if not wiped.ok:
        raise fp.FaultPreconditionError(f"could not clear {MOUNT}: {wiped.output}")

    init = fp.exec_in(
        SERVICE,
        f"setpriv --reuid=postgres --regid=postgres --init-groups -- "
        f"initdb -D {MOUNT} --encoding=UTF8 --locale=C --auth=trust --username=postgres",
        timeout_s=START_TIMEOUT_S,
    )
    if not init.ok:
        raise fp.FaultPreconditionError(f"initdb failed: {init.output}")

    checksums = fp.exec_in(SERVICE, f"pg_controldata {MOUNT} | grep -i 'checksum version'")
    if "1" not in checksums.stdout:
        raise fp.FaultPreconditionError(
            "data checksums are off in this cluster, so a torn page would be served "
            f"silently and the detection half of this suite is untestable: {checksums.stdout!r}"
        )


def relation_page_path() -> str:
    """The backing-store path of the test table's first page."""
    rel = psql("SELECT pg_relation_filepath('t')")
    if not rel:
        raise fp.FaultPreconditionError("could not resolve the relation file path")
    return f"{ROOT}/{rel}"


def page_markers(page: str) -> tuple[bool, bool]:
    """Which tuple versions the backing page holds.

    Read from the backing store, never through the mount: the mount serves
    un-fsynced data out of LazyFS's cache and would report the page whole.
    """
    # `grep -c` already prints 0 when nothing matches, and exits 1 while doing
    # it. A `|| echo 0` fallback therefore emits a second line and the counts
    # come back misaligned, so the exit status is swallowed instead.
    probe = fp.exec_in(
        SERVICE,
        f"grep -ac {OLD_MARKER} {page} 2>/dev/null || true; "
        f"grep -ac {NEW_MARKER} {page} 2>/dev/null || true",
    )
    counts = [line.strip() for line in probe.stdout.strip().splitlines() if line.strip()]
    if len(counts) != 2:
        raise fp.FaultEffectNotObserved(f"could not read markers from {page}: {probe.output!r}")
    return counts[0] != "0", counts[1] != "0"


def remount_lazyfs() -> None:
    """Bring the filesystem back after the tear crashed it.

    The container is recreated rather than restarted in place. LazyFS died with
    the mount still registered, so every path under it returns ENOTCONN, and
    the entrypoint's own stale-mount handling is the tested path for clearing
    that -- reproducing it here would be a second implementation of it.
    """
    fp.exec_in(SERVICE, "pkill -9 -u postgres postgres || true")
    recreate = fp.run(
        f"docker compose --profile tornwrite up -d --force-recreate --wait {SERVICE}",
        timeout_s=180.0,
    )
    if not recreate.ok:
        raise fp.FaultPreconditionError(f"could not recreate {SERVICE}: {recreate.output}")
    fp.verify_lazyfs_mounted(SERVICE, MOUNT)
    fp.verify_lazyfs_fault_channel(SERVICE)


def run_cycle(*, weakened: bool) -> Outcome:
    """Tear one heap page, then recover, and report what happened."""
    outcome = Outcome(weakened=weakened)

    reset_cluster()
    pg_ctl("-w start")

    psql("CREATE TABLE t(id int primary key, v text)")
    psql(f"INSERT INTO t VALUES (1, repeat('{OLD_MARKER}', 8))")

    if weakened:
        psql("ALTER SYSTEM SET full_page_writes = off")
        psql("SELECT pg_reload_conf()")

    # Settle the page before arming: the tear fires on the very next write to
    # the file, and a checkpoint here means that write is the one the UPDATE
    # below causes rather than any leftover from INSERT.
    psql("CHECKPOINT")

    fpw = psql("SHOW full_page_writes")
    expected_fpw = "off" if weakened else "on"
    if fpw != expected_fpw:
        raise fp.FaultPreconditionError(
            f"full_page_writes is {fpw!r}, expected {expected_fpw!r}; the inversion "
            f"would not be an inversion"
        )
    outcome.notes.append(f"full_page_writes={fpw}")

    page = relation_page_path()
    had_old, had_new = page_markers(page)
    if not had_old or had_new:
        raise fp.FaultPreconditionError(
            f"the settled page does not look like a clean starting state "
            f"(old={had_old}, new={had_new})"
        )

    fp.arm_torn_write(SERVICE, page, parts=2, persist=(1,))

    # Dirty the page in shared buffers, then force the write that gets torn.
    # The CHECKPOINT is expected to fail: LazyFS crashes mid-write, so the
    # command that provoked it cannot return success.
    psql(f"UPDATE t SET v = repeat('{NEW_MARKER}', 8) WHERE id = 1")
    psql("CHECKPOINT", expect_ok=False)

    outcome.torn_bytes = fp.verify_torn_write_injected(
        SERVICE, page, expected_bytes=HALF_PAGE_BYTES
    )
    outcome.backing_has_old, outcome.backing_has_new = page_markers(page)

    if not outcome.tore:
        raise fp.FaultEffectNotObserved(
            f"no torn page in the backing store after the fault fired "
            f"(persisted={outcome.torn_bytes}, old={outcome.backing_has_old}, "
            f"new={outcome.backing_has_new}). Every assertion below would be vacuous."
        )

    remount_lazyfs()

    started = pg_ctl("-w start", expect_ok=False)
    outcome.postgres_started = started.ok
    if started.ok:
        row = fp.exec_in(
            SERVICE,
            f"setpriv --reuid=postgres --regid=postgres --init-groups -- "
            f'psql -U postgres -At -q -c "SELECT left(v, {len(NEW_MARKER)}) FROM t WHERE id = 1"',
        )
        outcome.row_value = row.stdout.strip() if row.ok else None
        outcome.checksum_error = "checksum" in row.output.lower()
    else:
        logs = fp.exec_in(SERVICE, "cat /tmp/pg.log 2>/dev/null || true")
        outcome.checksum_error = "page verification failed" in logs.stdout.lower()
        outcome.notes.append("postgres refused to start after the tear")

    return outcome


def assert_not_silently_accepted(outcome: Outcome) -> None:
    """The contract: repaired or detected, never silently wrong."""
    if outcome.repaired:
        return
    if outcome.detected:
        return
    raise TornWriteViolation(
        f"a torn page was neither repaired nor detected: the row read "
        f"{outcome.row_value!r}, expected {NEW_MARKER!r} from a repaired page or a "
        f"checksum failure from a detected one. PostgreSQL served data that was "
        f"never written."
    )


def backing_path(relative: str) -> str:
    """A PGDATA-relative path as the backing store has it."""
    return f"{ROOT}/{relative}"


def current_wal_segment() -> str:
    """PGDATA-relative path of the WAL segment being written right now."""
    name = psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    if not name:
        raise fp.FaultPreconditionError("could not resolve the current WAL segment")
    return f"pg_wal/{name}"


def durable_rows() -> set[int]:
    """Rows PostgreSQL acknowledged with synchronous_commit on.

    Read back after recovery. A commit acked under `synchronous_commit=on` has
    had its WAL flushed, so it must survive; a torn WAL record can only ever
    cost a commit that was never acknowledged.
    """
    out = psql(f"SELECT k FROM {WAL_TABLE} ORDER BY k", expect_ok=False)
    return {int(line) for line in out.splitlines() if line.strip().isdigit()}


def run_metadata_cycle(*, target: str, mangle: bool) -> MetadataTearJson:
    """Tear (or mangle) the WAL or the control file, then recover.

    Neither structure has PostgreSQL's full-page-image safety net, and neither
    can be repaired from elsewhere in the cluster, so the contract is narrower
    than for a heap page: every commit that was *acknowledged* must still be
    there, and anything PostgreSQL cannot vouch for it must refuse rather than
    serve. A commit that was never acked may legitimately vanish -- that is
    what an un-flushed WAL record means.

    `mangle` is the inversion. Overwriting the file with random bytes is past
    what a tear does and must be noticed; if it is not, then the observable a
    green run is measured on is one that damage never reaches.
    """
    reset_cluster()
    pg_ctl("-w start")
    psql(f"CREATE TABLE {WAL_TABLE} (k int PRIMARY KEY)")
    psql("SET synchronous_commit = on")
    for key in range(50):
        psql(f"INSERT INTO {WAL_TABLE} VALUES ({key})")
    psql("CHECKPOINT")
    acked = durable_rows()

    relative = current_wal_segment() if target == "wal" else "global/pg_control"
    path = backing_path(relative)

    if mangle:
        damaged = fp.exec_in(
            SERVICE, f"dd if=/dev/urandom of={path} bs=4096 count=4 conv=notrunc 2>/dev/null"
        )
        if not damaged.ok:
            raise fp.FaultPreconditionError(f"could not mangle {relative}: {damaged.output}")
        torn = None
    else:
        # pg_control needs a finer split than a WAL segment. Its CRC covers
        # only the first few hundred bytes, so a tear at the halfway mark
        # leaves that whole region either wholly stale or wholly fresh --
        # payload and CRC together -- and the file stays internally consistent,
        # merely older. That is by design: the payload fits in one sector,
        # which is why PostgreSQL treats control-file writes as atomic.
        #
        # Splitting 8192 into 32 parts puts the boundary at 256 bytes, inside
        # the CRC-covered region, so the surviving head and the stale bytes
        # after it disagree and the checksum has something to catch.
        parts, keep = (32, (1,)) if target == "control" else (2, (1,))
        fp.arm_torn_write(SERVICE, path, parts=parts, persist=keep)
        # Drive writes into the armed file. A commit writes WAL; a checkpoint
        # rewrites pg_control.
        for key in range(50, 120):
            psql(f"INSERT INTO {WAL_TABLE} VALUES ({key})", expect_ok=False)
        psql("CHECKPOINT", expect_ok=False)
        time.sleep(2)
        try:
            torn = fp.verify_torn_write_injected(SERVICE, path)
        except fp.FaultEffectNotObserved:
            torn = None

    remount_lazyfs()
    started = pg_ctl("-w start", expect_ok=False)

    survived: set[int] = set()
    complaint = ""
    if started.ok:
        survived = durable_rows()
    else:
        # pg_ctl reports the refusal on its own stderr as well as in the log,
        # and which one carries the detail varies by failure. Read both, or an
        # attributable refusal looks unattributable.
        logs = fp.exec_in(SERVICE, "cat /tmp/pg.log 2>/dev/null || true")
        lowered = (logs.output + started.output).lower()
        for marker in (
            "checksum",
            "invalid",
            "corrupt",
            "could not",
            "incorrect",
            "control file",
            "database system was not",
            "fatal",
        ):
            if marker in lowered:
                complaint = marker
                break

    return MetadataTearJson(
        target=target,
        mangled=mangle,
        file=relative,
        torn_bytes=torn,
        acked_before=len(acked),
        surviving=len(survived),
        lost_acked=sorted(acked - survived)[:20],
        postgres_started=started.ok,
        complaint=complaint,
        detected=bool(complaint),
        contracts=["W1", "R2"],
    )


app = typer.Typer(add_completion=False)


@app.command()
def main(
    prove_oracle: bool = typer.Option(
        False,
        "--prove-oracle",
        help="Run the inversion first and require it to go red.",
    ),
    target: str = typer.Option(
        "heap",
        "--target",
        help="heap page, WAL segment, or the control file",
    ),
) -> None:
    """Tear a PostgreSQL structure and assert it is never served silently."""
    fp.verify_lazyfs_mounted(SERVICE, MOUNT)
    fp.verify_lazyfs_fault_channel(SERVICE)

    if target not in ("heap", "wal", "control"):
        raise typer.BadParameter(f"unknown target {target!r}")

    if target in ("wal", "control"):
        if prove_oracle:
            red = run_metadata_cycle(target=target, mangle=True)
            print(json.dumps(red, indent=2))
            if not red["detected"]:
                raise OracleNotProven(
                    f"a {target} file overwritten with random bytes produced no "
                    f"attributable complaint (started={red['postgres_started']}). "
                    f"Refusing to start without naming the damage is not proof the "
                    f"damage was noticed -- it is indistinguishable from the harness "
                    f"having broken something unrelated, which is exactly the false "
                    f"green a green run below would then inherit."
                )
            print(f"oracle proven: a mangled {target} file is not silently accepted\n")

        meta = run_metadata_cycle(target=target, mangle=False)
        print(json.dumps(meta, indent=2))
        if meta["torn_bytes"] is None:
            raise fp.FaultEffectNotObserved(
                f"no torn write landed on the {target} file, so this run asserts "
                f"nothing. The fault arms for the next write to that path."
            )
        # Order matters. A refusal makes every row unreadable, so checking for
        # missing commits first would report a correct detection as data loss.
        # The contract is repaired-or-detected: if PostgreSQL serves the
        # database at all, every acked commit must be in it; if it refuses, it
        # must say what it found.
        if meta["postgres_started"]:
            if meta["lost_acked"]:
                raise TornWriteViolation(
                    f"PostgreSQL started after a torn {target} write and is missing "
                    f"acked commits: {meta['lost_acked']}. A commit acknowledged under "
                    f"synchronous_commit=on did not survive, and nothing refused to "
                    f"serve the database."
                )
        elif not meta["detected"]:
            raise TornWriteViolation(
                f"PostgreSQL will not start after a torn {target} write and gave no "
                f"reason that names the damage; the failure is unattributable and "
                f"could as easily be unrelated breakage as a caught tear."
            )
        if meta["postgres_started"]:
            print(f"torn {target} write: PostgreSQL recovered; no acked commit lost")
        else:
            print(
                f"torn {target} write: PostgreSQL refused to serve it, "
                f"reporting {meta['complaint']!r}; never silently accepted"
            )
        return

    if prove_oracle:
        weakened = run_cycle(weakened=True)
        print(json.dumps(weakened.as_json(), indent=2))
        if not weakened.tore:
            raise OracleNotProven("the inversion never tore the page")
        if weakened.repaired:
            raise OracleNotProven(
                "with full_page_writes=off the torn page was still repaired, which "
                "means the tear did not reach PostgreSQL's data path. A green result "
                "from the real run would prove nothing."
            )
        if not weakened.detected:
            raise OracleNotProven(
                "with full_page_writes=off the torn page was neither repaired nor "
                "detected -- data checksums did not fire. That is itself the "
                "violation this suite exists to catch."
            )
        print(
            "oracle proven: with full_page_writes off, the tear survived and checksums caught it\n"
        )

    outcome = run_cycle(weakened=False)
    print(json.dumps(outcome.as_json(), indent=2))
    assert_not_silently_accepted(outcome)

    verdict = "repaired by the full-page image" if outcome.repaired else "detected by checksums"
    print(f"torn page {verdict}; never silently accepted")


if __name__ == "__main__":
    try:
        app()
    except (TornWriteViolation, OracleNotProven, fp.FaultError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
