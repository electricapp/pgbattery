#!/usr/bin/env -S uv run --project testing python
"""Thin subprocess wrapper around the Elle uberjar.

All Jepsen-format encoding decisions live in the workers
(`txn_worker_loop`, `list_append_worker_loop` in `linearizability_register.py`).
This module is intentionally dumb: it takes a list of records already in
Elle's expected JSON shape, writes them to disk, runs `java -jar
elle-cli-standalone.jar`, parses the result. No format conversion, no
intent inference, no time fudging.

If you find yourself adding policy here, ask whether it belongs in the
worker instead. The whole point of the redesign is that exactly one place
decides what an op record looks like.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

JAR_PATH: Path = (
    Path(__file__).resolve().parent / "third_party" / "elle" / "elle-cli-standalone.jar"
)
DEFAULT_TIMEOUT_S: int = 300
JVM_HEAP_MAX: str = "2g"


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ElleAnomaly:
    """One anomaly Elle found.

    `name` is the anomaly class ("G0", "G1a", "G-single", "lost-update", ...).
    `cycle` is the list of operation indices forming the dependency cycle,
    when Elle provides one.
    `detail` is the raw Elle map for this instance, for deep debugging.
    """

    name: str
    cycle: list[int]
    detail: dict[str, object]


@dataclass
class ElleResult:
    """Parsed output of one Elle invocation."""

    valid: bool | None  # True = valid, False = invalid, None = unknown
    anomalies: list[ElleAnomaly]
    elapsed_ms: float
    op_count: int
    raw: dict[str, object] = field(default_factory=dict)

    @property
    def anomaly_summary(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for a in self.anomalies:
            out[a.name] = out.get(a.name, 0) + 1
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Result parsing
# ─────────────────────────────────────────────────────────────────────────────


class ElleError(RuntimeError):
    """Infrastructure failure: jar missing, JVM crash, parse error.
    Distinct from 'Elle found anomalies' which is a normal result."""


def _parse_valid(raw: dict[str, object]) -> bool | None:
    """Elle's :valid? is true, false, or :unknown (-> "unknown" in JSON)."""
    v = raw.get("valid?")
    if v is True:
        return True
    if v is False:
        return False
    return None


def _parse_anomalies(raw: dict[str, object]) -> list[ElleAnomaly]:
    out: list[ElleAnomaly] = []
    anomalies = raw.get("anomalies")
    if not isinstance(anomalies, dict):
        return out
    for name, instances in anomalies.items():
        if not isinstance(instances, list):
            out.append(ElleAnomaly(name=str(name), cycle=[], detail={"raw": instances}))
            continue
        for inst in instances:
            if not isinstance(inst, dict):
                out.append(ElleAnomaly(name=str(name), cycle=[], detail={"raw": inst}))
                continue
            cycle_raw = inst.get("cycle") or inst.get("steps") or []
            if isinstance(cycle_raw, list):
                cycle = [c if isinstance(c, int) else hash(repr(c)) for c in cycle_raw]
            else:
                cycle = []
            out.append(ElleAnomaly(name=str(name), cycle=cycle, detail=inst))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Subprocess driver
# ─────────────────────────────────────────────────────────────────────────────


def check_with_elle(
    history_path: Path,
    model: str = "rw-register",
    timeout_s: int = DEFAULT_TIMEOUT_S,
    jar_path: Path | None = None,
    stderr_log: Path | None = None,
) -> ElleResult:
    """Drive `java -jar elle-cli-standalone.jar <model> <history-path>`.

    Args:
        history_path: JSON file containing a list of Jepsen-format records
            (see `JepsenRecord` in linearizability_register.py).
        model:        'rw-register' | 'list-append'.
        timeout_s:    subprocess wall-clock cap.
        jar_path:     override default jar path (CI overrides).
        stderr_log:   if provided, dump JVM stderr here for debugging.

    Raises:
        ElleError: jar missing, history missing, subprocess timeout, JVM
            crash, or unparseable output.
    """
    jar = jar_path or JAR_PATH
    if not jar.exists():
        raise ElleError(f"Elle uberjar not found at {jar}. Build via: ./testing/build_elle.sh")
    if not history_path.exists():
        raise ElleError(f"History file does not exist: {history_path}")

    cmd = [
        "java",
        f"-Xmx{JVM_HEAP_MAX}",
        # Elle transitively loads rhizome.viz (graph rendering), whose class
        # initializer touches AWT and throws HeadlessException on a headless
        # runner. We never render graphs, so force headless mode.
        "-Djava.awt.headless=true",
        "-jar",
        str(jar),
        model,
        str(history_path),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise ElleError(f"Elle subprocess timed out after {timeout_s}s") from e
    except FileNotFoundError as e:
        raise ElleError(f"`java` not on PATH: {e}") from e

    if stderr_log is not None:
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        stderr_log.write_text(proc.stderr or "", encoding="utf-8")

    if proc.returncode == 2:
        raise ElleError(f"Elle subprocess failed (exit 2). stderr:\n{proc.stderr}")
    if proc.returncode not in (0, 1):
        raise ElleError(
            f"Elle subprocess unexpected exit {proc.returncode}. stderr:\n{proc.stderr}"
        )
    if not proc.stdout.strip():
        raise ElleError(f"Elle produced no stdout. stderr:\n{proc.stderr}")

    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise ElleError(f"Elle stdout was not valid JSON: {e}. stdout:\n{proc.stdout[:500]}") from e
    if not isinstance(raw, dict):
        raise ElleError(f"Elle JSON was not an object: {type(raw)}")

    meta = raw.get("_meta") if isinstance(raw.get("_meta"), dict) else {}
    op_count_raw = meta.get("op-count") if isinstance(meta, dict) else 0
    op_count = int(op_count_raw) if isinstance(op_count_raw, int) else 0
    elapsed_raw = raw.get("elapsed-ms", 0.0)
    elapsed_ms = float(elapsed_raw) if isinstance(elapsed_raw, (int, float)) else 0.0

    return ElleResult(
        valid=_parse_valid(raw),
        anomalies=_parse_anomalies(raw),
        elapsed_ms=elapsed_ms,
        op_count=op_count,
        raw=raw,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Top-level convenience
# ─────────────────────────────────────────────────────────────────────────────


def run_check(
    records: list[dict[str, object]],
    out_dir: Path,
    model: str = "rw-register",
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> ElleResult:
    """Write `records` to disk as Elle JSON, run Elle, return result.

    `records` must already be in Jepsen / Elle format -- this function
    does no conversion. Each record should have integer `time` (nanoseconds),
    integer `process`, and `type` in {"invoke", "ok", "fail", "info"}.

    Artifacts written under `out_dir`:
        history.elle.json   - the records, as-is
        elle_result.json    - parsed ElleResult
        elle_stderr.log     - JVM stderr
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    history_path = out_dir / "history.elle.json"
    history_path.write_text(json.dumps(records, indent=0), encoding="utf-8")

    result = check_with_elle(
        history_path,
        model=model,
        timeout_s=timeout_s,
        stderr_log=out_dir / "elle_stderr.log",
    )

    (out_dir / "elle_result.json").write_text(
        json.dumps(
            {
                "valid": result.valid,
                "anomaly_summary": result.anomaly_summary,
                "anomalies": [
                    {"name": a.name, "cycle": a.cycle, "detail": a.detail} for a in result.anomalies
                ],
                "elapsed_ms": result.elapsed_ms,
                "op_count": result.op_count,
                "raw": result.raw,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return result
