#!/usr/bin/env -S uv run --project testing python
"""Aggregate per-attack Elle results into a single Markdown summary table.

Reads testing/artifacts/elle-*/ (matrix_meta.json, results.json,
elle_result.json, fault_waves.json, history.elle.json) and writes
testing/artifacts/elle-summary.md with one row per attack.

This is also the evidence gate: a green summary has to mean "the cluster was
tortured and Elle could not falsify it", never "nothing ran". A row is an
infrastructure ERROR — not a PASS — when

  - artifacts are missing, or an expected attack has no directory at all;
  - the history is empty or implausibly thin for the configured workload
    (see MIN_OK_TXNS_PER_WORKER_SECOND);
  - the fault waves the run planned never landed;
  - the run used a different density profile than the caller expected.

Elle's `:unknown` verdict is a failure, not a pass: an indeterminate history
tells us nothing about strict serializability.

Environment:
  ELLE_EXPECT_ATTACKS  Space-separated attacks that MUST have artifacts.
                       Absent ones become ERROR rows instead of vanishing.
  ELLE_EXPECT_PROFILE  Density profile every attack must have run with.
  ELLE_MIN_OK_PER_WORKER_SECOND
                       Override the committed-transaction floor. The default is
                       calibrated for CI (Linux, native docker); Docker
                       Desktop's loopback proxy is roughly 50x slower, so a
                       healthy laptop run needs a lower floor rather than a
                       silently disabled one.

Exit codes:
  0 - every attack passed on usable evidence
  1 - at least one attack found an anomaly
  2 - at least one attack had an infrastructure failure
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from pydantic import BaseModel, ConfigDict, RootModel

import artifact_models as am

ARTIFACT_ROOT = Path("testing/artifacts")
SUMMARY_OUT = ARTIFACT_ROOT / "elle-summary.md"

MIN_OK_TXNS_PER_WORKER_SECOND: Final[float] = 0.5
"""Completed-transaction floor, per worker per second of workload.

Elle can only find a cycle among transactions that actually committed, so the
meaningful floor is on `:ok` records, not on the raw record count — a run where
every worker is wedged against a dead gateway emits tens of thousands of
`:invoke`/`:info` pairs per second and would sail past any total-ops check.

Measured healthy rate (CI run 30495947519, list-append, 2 keys per txn,
SERIALIZABLE, synchronous replication, GitHub 2-core runner): 12.5 committed
txns per worker-second. The floor is 0.5 — one commit every two worker-seconds,
4% of the measured rate — so it trips only when the cluster was unavailable for
essentially the whole workload, not because a runner was slow or a chaos window
ran long.

Calibrated for CI. Docker Desktop on macOS routes the workload through its
loopback proxy and measures ~0.22 commits per worker-second on the same
workload, so local runs need ELLE_MIN_OK_PER_WORKER_SECOND rather than a
weaker gate in CI.
"""

MIN_OK_TXNS_FLOOR: Final[int] = 20
"""Absolute floor, so a very short manual run can't derive its way to zero."""

THIN_OK_FRACTION: Final[float] = 0.25
"""Below this share of completed transactions the history is mostly
indeterminate: reported loudly as a note, since Elle's verdict on such a
history is close to vacuous."""

_TYPE_LINE_PREFIX: Final[str] = '"type": "'


class _JepsenRecordType(BaseModel):
    """One history record, read only for the field the count needs."""

    model_config = ConfigDict(extra="ignore")

    type: str = ""


class _RecordTypes(RootModel[list[_JepsenRecordType]]):
    """A whole history, when the line scan found nothing to count."""


def _format_anomalies(summary: dict[str, int]) -> str:
    if not summary:
        return "-"
    return ", ".join(f"{name}x{count}" for name, count in sorted(summary.items()))


def _verdict_label(valid: bool | None) -> str:
    if valid is True:
        return "PASS"
    if valid is False:
        return "FAIL"
    return "UNKNOWN"


def _fixed_width_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Pipe-table with each cell padded to its column's max width.

    Markdown renderers ignore the padding, but raw text views (terminal,
    less, `cat`) get aligned columns.
    """
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in rows)) if rows else len(headers[i])
        for i in range(len(headers))
    ]

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells)) + " |"

    lines = [fmt_row(headers)]
    lines.append("|" + "|".join("-" * (w + 2) for w in widths) + "|")
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def _count_record_types(path: Path) -> dict[str, int]:
    """Count Jepsen record types in an Elle history without loading it.

    `elle_adapter.run_check` writes the history with `json.dumps(indent=0)`,
    which puts every key on its own line, so an exact count is a line scan —
    histories run to hundreds of MB once the workload is dense. Falls back to a
    full parse if the scan finds no type keys, so a formatting change surfaces
    as slow rather than as a false "empty history".
    """
    counts: Counter[str] = Counter()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped.startswith(_TYPE_LINE_PREFIX):
                counts[stripped[len(_TYPE_LINE_PREFIX) :].rstrip(",").rstrip('"')] += 1
    if not counts:
        for rec in _RecordTypes.model_validate(json.loads(path.read_text(encoding="utf-8"))).root:
            counts[rec.type] += 1
    out = {k: v for k, v in counts.items()}
    out["total"] = sum(counts.values())
    return out


def _history_stats(d: Path) -> am.HistoryStats | None:
    """Record-type counts for one attack dir, computed once and cached.

    The history itself is only uploaded from CI on failure, so the cached
    counts are what a downstream verdict job re-aggregates from.
    """
    history = d / "history.elle.json"
    cache = d / "history_stats.json"
    if history.exists():
        stats = _count_record_types(history)
        cache.write_text(json.dumps(stats, indent=2), encoding="utf-8")
        return am.HistoryStats.model_validate(stats)
    return am.load(am.HistoryStats, cache)


def _min_ok_rate() -> float:
    raw = os.environ.get("ELLE_MIN_OK_PER_WORKER_SECOND", "").strip()
    if not raw:
        return MIN_OK_TXNS_PER_WORKER_SECOND
    try:
        rate = float(raw)
    except ValueError:
        print(
            f"ELLE_MIN_OK_PER_WORKER_SECOND={raw!r} is not a number; "
            f"using {MIN_OK_TXNS_PER_WORKER_SECOND}",
            file=sys.stderr,
        )
        return MIN_OK_TXNS_PER_WORKER_SECOND
    if rate <= 0:
        print(
            f"ELLE_MIN_OK_PER_WORKER_SECOND={rate} would disable the floor; "
            f"using {MIN_OK_TXNS_PER_WORKER_SECOND}",
            file=sys.stderr,
        )
        return MIN_OK_TXNS_PER_WORKER_SECOND
    return rate


def _ok_floor(workers: float, duration: float) -> int:
    return max(MIN_OK_TXNS_FLOOR, round(_min_ok_rate() * workers * duration))


@dataclass
class AttackRow:
    """One matrix cell: what Elle said, plus whether we believe the evidence."""

    attack: str
    seed: str = "-"
    ops: str = "-"
    ok: str = "-"
    faults: str = "-"
    valid: str = "ERROR"
    anomalies: str = "-"
    elle_ms: str = "-"
    errors: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _check_fault_waves(d: Path, meta: am.MatrixMeta | None, row: AttackRow) -> None:
    """Assert the run injected the faults it planned.

    Wave 1 is the harness's own injector, started unconditionally at
    `--fault-at`; it leaves no artifact of its own, so it is counted as planned
    and attempted. Waves 2..N come from run_elle_matrix.sh's sibling driver,
    which records every injection in fault_waves.json.
    """
    waves = am.load(am.FaultWaves, d / "fault_waves.json")
    if waves is None:
        row.errors.append("missing fault_waves.json — the fault-wave driver never ran")
        return

    row.faults = f"{len(waves.injected) + 1}/{len(waves.planned) + 1}"

    if not waves.marker_seen:
        row.errors.append(
            "fault-wave driver never saw the workload start; extra fault waves did not run"
        )
    elif len(waves.injected) != len(waves.planned):
        row.errors.append(
            f"only {len(waves.injected)} of {len(waves.planned)} extra fault waves were injected"
        )

    if waves.ineffective:
        offsets = ", ".join(f"t={w.offset_s}s" for w in waves.ineffective)
        row.notes.append(
            f"{len(waves.ineffective)} fault wave(s) found no leader to hit ({offsets}) — "
            "that much of the schedule tested nothing"
        )

    if meta is not None and meta.fault_waves_dropped > 0:
        row.notes.append(
            f"{meta.fault_waves_dropped} fault wave(s) were dropped at plan time because the "
            "workload duration was too short for them"
        )


def _check_history_volume(
    d: Path,
    results: am.RunResults | None,
    meta: am.MatrixMeta | None,
    row: AttackRow,
) -> None:
    """Assert the history is big enough to have been able to falsify anything."""
    stats = _history_stats(d)
    if stats is None:
        row.errors.append("missing history.elle.json and history_stats.json")
        return

    total = stats.total
    ok = stats.ok
    row.ops = str(total)
    row.ok = str(ok)

    # results.json is what the harness actually ran with; matrix_meta.json is
    # what the matrix asked for, and is still there when the harness died.
    workers = (results.workers if results is not None else 0.0) or (
        meta.workers if meta is not None else 0.0
    )
    duration = (results.duration_s if results is not None else 0.0) or (
        meta.duration_s if meta is not None else 0.0
    )
    if workers <= 0 or duration <= 0:
        row.errors.append("no workers/duration_s recorded — cannot derive a committed-txn floor")
        return

    floor = _ok_floor(workers, duration)
    if total == 0:
        row.errors.append("history is empty — the workload recorded nothing")
        return
    if ok < floor:
        row.errors.append(
            f"only {ok} committed txns in the history; floor for "
            f"{workers:.0f} workers x {duration:.0f}s is {floor} "
            f"({_min_ok_rate()}/worker-second). Too few committed transactions for "
            "Elle to have been able to falsify anything"
        )
        return
    if ok < total * THIN_OK_FRACTION:
        row.notes.append(
            f"{ok} of {total} records committed ({100.0 * ok / total:.1f}%) — the history is "
            "mostly indeterminate ops, so Elle's verdict rests on a thin slice of it"
        )


def _row_for(attack: str, d: Path | None, expect_profile: str) -> AttackRow:
    row = AttackRow(attack=attack)
    if d is None:
        row.errors.append("no artifact dir — the attack never ran")
        row.anomalies = "attack did not run"
        return row

    meta = am.load(am.MatrixMeta, d / "matrix_meta.json")
    results = am.load(am.RunResults, d / "results.json")
    elle = am.load(am.ElleArtifact, d / "elle_result.json")

    seed = (meta.seed if meta is not None else None) or (
        results.seed if results is not None else None
    )
    if seed is None:
        row.errors.append("no seed recorded in matrix_meta.json or results.json — not replayable")
    else:
        row.seed = str(seed)

    if meta is None:
        row.errors.append("missing matrix_meta.json — dir was not produced by run_elle_matrix.sh")
    elif expect_profile and meta.profile != expect_profile:
        row.errors.append(f"ran with profile {meta.profile!r}, expected {expect_profile!r}")

    if results is None:
        row.errors.append("missing results.json — the harness never finished")

    _check_fault_waves(d, meta, row)
    _check_history_volume(d, results, meta, row)

    if elle is None:
        row.errors.append("missing elle_result.json — Elle never produced a verdict")
        row.anomalies = "missing elle_result.json"
        return row

    verdict = _verdict_label(elle.valid)
    row.anomalies = _format_anomalies(elle.anomaly_summary)
    row.elle_ms = f"{elle.elapsed_ms:.0f}"
    # Elle's own op count is a cross-check on the history we counted, not a
    # substitute for it: it reports what the checker parsed.
    if elle.op_count is not None and row.ops not in ("-", str(elle.op_count)):
        row.notes.append(f"Elle parsed {elle.op_count} records, history holds {row.ops}")
    # The evidence gate outranks the verdict: a PASS on an unusable history is
    # exactly the "green means nothing was tested" failure this file exists to
    # prevent.
    row.valid = "ERROR" if row.errors else verdict
    return row


def _density_line(metas: list[am.MatrixMeta]) -> str:
    shapes = {
        (
            m.profile,
            f"{m.workers:g}",
            str(m.keys),
            f"{m.duration_s:g}",
            str(m.fault_waves_planned),
        )
        for m in metas
    }
    if len(shapes) != 1:
        return f"Density: mixed across {len(shapes)} configurations — see each matrix_meta.json."
    profile, workers, keys, duration, waves = shapes.pop()
    instants = "fault instant" if waves == "1" else "fault instants"
    return (
        f"Profile `{profile}` — {workers} workers, {keys} keys, {duration}s workload, "
        f"{waves} {instants} per run."
    )


def main() -> int:
    expect_attacks = os.environ.get("ELLE_EXPECT_ATTACKS", "").split()
    expect_profile = os.environ.get("ELLE_EXPECT_PROFILE", "").strip()

    dirs = {
        p.name.removeprefix("elle-"): p for p in sorted(ARTIFACT_ROOT.glob("elle-*")) if p.is_dir()
    }
    if not dirs and not expect_attacks:
        print(f"No elle-* artifact dirs under {ARTIFACT_ROOT}", file=sys.stderr)
        return 2

    # Expected order first (that is the matrix order), then anything else found.
    attacks = list(dict.fromkeys([*expect_attacks, *sorted(dirs)]))
    rows = [_row_for(a, dirs.get(a), expect_profile) for a in attacks]
    metas = [
        m
        for m in (am.load(am.MatrixMeta, d / "matrix_meta.json") for d in dirs.values())
        if m is not None
    ]

    headers = ["Attack", "Seed", "Ops", "Ok txns", "Faults", "Valid", "Anomalies", "Elle ms"]
    table_rows = [
        [r.attack, r.seed, r.ops, r.ok, r.faults, r.valid, r.anomalies, r.elle_ms] for r in rows
    ]

    lines = [
        "# Elle x Attack Matrix",
        "",
        "Per-attack consistency check results from `testing/run_elle_matrix.sh`.",
        "Strict-serializable model via Elle v0.2.2.",
        "",
        _density_line(metas),
        "Replay a row with "
        "`ELLE_SEED=<seed> ELLE_PROFILE=<profile> testing/run_elle_matrix.sh <attack>`.",
        "",
    ]
    lines.extend(_fixed_width_table(headers, table_rows))
    lines.append("")

    errors = [(r.attack, e) for r in rows for e in r.errors]
    if errors:
        lines.append("## Infrastructure errors")
        lines.append("")
        lines.extend(f"- **{attack}**: {err}" for attack, err in errors)
        lines.append("")

    notes = [(r.attack, n) for r in rows for n in r.notes]
    if notes:
        lines.append("## Notes")
        lines.append("")
        lines.extend(f"- **{attack}**: {note}" for attack, note in notes)
        lines.append("")

    lines.append(
        "Legend: PASS = strict-serializable; FAIL = Elle found anomaly cycles; "
        "UNKNOWN = Elle returned indeterminate; ERROR = infrastructure failure "
        "(missing artifacts, unusable history, or faults that never landed)."
    )
    lines.append("")

    SUMMARY_OUT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWrote {SUMMARY_OUT}")

    if errors:
        return 2
    if any(r.valid != "PASS" for r in rows):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
