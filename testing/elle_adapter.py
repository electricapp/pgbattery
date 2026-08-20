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

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

JAR_PATH: Path = (
    Path(__file__).resolve().parent / "third_party" / "elle" / "elle-cli-standalone.jar"
)
DEFAULT_TIMEOUT_S: int = 300
JVM_HEAP_MAX: str = "4g"
"""A list-append read returns the whole list, so history size grows with the
square of the committed count: the `full` profile's densest attacks produce
histories in the hundreds of MB, and Elle holds the parsed graph in memory."""


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


def _is_index(entry: object) -> bool:
    """Whether a cycle entry is an op index rather than a whole op map."""
    match entry:
        case bool():
            return False
        case int():
            return True
        case _:
            return False


def _as_instance(entry: object) -> object:
    """A payload entry, in the mapping shape `AnomalyInstance` reads.

    A flag anomaly is a bare scalar rather than a map. Wrapping it keeps it in
    the report instead of dropping it for not being the usual shape.
    """
    match entry:
        case dict():
            return entry
        case _:
            return {"payload": entry}


def _as_instances(payload: object) -> list[object]:
    """One anomaly class's payload, as a list of instances."""
    match payload:
        case list():
            return [_as_instance(entry) for entry in payload]
        case _:
            return [_as_instance(payload)]


class AnomalyInstance(BaseModel):
    """One instance of one anomaly class.

    `cycle` and `steps` are Elle's two spellings for the ops forming a cycle.
    Anything else it attaches is kept, so a payload this module does not model
    still reaches the report.
    """

    model_config = ConfigDict(extra="allow")

    cycle: list[int] = []
    steps: list[int] = []

    @field_validator("cycle", "steps", mode="before")
    @classmethod
    def _indices_only(cls, value: object) -> object:
        """Some anomaly classes spell a step as the whole op map. Only an index
        identifies an op to a reader, so anything else is not carried as one."""
        match value:
            case list():
                return [entry for entry in value if _is_index(entry)]
            case _:
                return []

    @property
    def indices(self) -> list[int]:
        return self.cycle or self.steps


class ElleMeta(BaseModel):
    """Elle's `_meta` block."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    op_count: int = Field(default=0, alias="op-count")


class ElleOutput(BaseModel):
    """Elle's JSON, parsed once at the boundary.

    Every Clojure-spelled key is bound to a Python name here, so nothing
    downstream reaches into a raw map or asks what shape it got.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    valid: bool | None = Field(default=None, alias="valid?")
    anomalies: dict[str, list[AnomalyInstance]] = {}
    elapsed_ms: float = Field(default=0.0, alias="elapsed-ms")
    meta: ElleMeta = Field(default_factory=ElleMeta, alias="_meta")

    @field_validator("valid", mode="before")
    @classmethod
    def _unknown_is_neither_verdict(cls, value: object) -> object:
        """`:unknown` means Elle could not decide, which is not `false`."""
        match value:
            case bool():
                return value
            case _:
                return None

    @field_validator("anomalies", mode="before")
    @classmethod
    def _normalise_payloads(cls, value: object) -> object:
        match value:
            case dict():
                return {str(name): _as_instances(payload) for name, payload in value.items()}
            case _:
                return {}

    @property
    def flat_anomalies(self) -> list[ElleAnomaly]:
        return [
            ElleAnomaly(name=name, cycle=inst.indices, detail=inst.model_dump())
            for name, instances in self.anomalies.items()
            for inst in instances
        ]


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
        output = ElleOutput.model_validate(json.loads(proc.stdout))
    except json.JSONDecodeError as e:
        raise ElleError(f"Elle stdout was not valid JSON: {e}. stdout:\n{proc.stdout[:500]}") from e
    except ValidationError as e:
        raise ElleError(f"Elle JSON was not the shape this adapter reads: {e}") from e

    return ElleResult(
        valid=output.valid,
        anomalies=output.flat_anomalies,
        elapsed_ms=output.elapsed_ms,
        op_count=output.meta.op_count,
        raw=output.model_dump(by_alias=True),
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
