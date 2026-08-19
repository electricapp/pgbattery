"""Typed models for the JSON artifacts the harnesses write and read back.

`aggregate_elle_results.py` is the evidence gate: it decides whether a green
row means "the cluster was tortured and Elle could not falsify it" or "nothing
ran". It used to make that decision out of `dict[str, object]` loaded from
disk, asking at each use site what type it had got — which is a parse spread
across a dozen call sites, and the place where a renamed field turns into a
default instead of an error.

Each model here names one artifact file. Missing and unreadable are the same
answer, `None`, because a gate that cannot read its evidence has none.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, ValidationError

ArtifactT = TypeVar("ArtifactT", bound=BaseModel)


class MatrixMeta(BaseModel):
    """`matrix_meta.json`, written by `run_elle_matrix.sh` before the run."""

    model_config = ConfigDict(extra="allow")

    attack: str = ""
    seed: int | None = None
    profile: str = ""
    workload: str = ""
    workers: float = 0.0
    keys: int = 0
    duration_s: float = 0.0
    fault_waves_planned: int = 0
    fault_waves_dropped: int = 0


class RunResults(BaseModel):
    """`results.json`, written by `linearizability_register.py` when it ends."""

    model_config = ConfigDict(extra="allow")

    seed: int | None = None
    workers: float = 0.0
    keys: int = 0
    duration_s: float = 0.0
    verdict: str = ""


class ElleArtifact(BaseModel):
    """`elle_result.json`, written by `elle_adapter.run_check`."""

    model_config = ConfigDict(extra="allow")

    valid: bool | None = None
    anomaly_summary: dict[str, int] = {}
    elapsed_ms: float = 0.0
    op_count: int | None = None


class FaultWave(BaseModel):
    """One entry of `fault_waves.json`'s `planned` or `injected` list."""

    model_config = ConfigDict(extra="allow")

    offset_s: float | None = None
    mode: str = ""
    effective: bool = False


class FaultWaves(BaseModel):
    """`fault_waves.json`, rewritten by the fault-wave driver after each wave."""

    model_config = ConfigDict(extra="allow")

    attack: str = ""
    marker_seen: bool = False
    planned: list[FaultWave] = []
    injected: list[FaultWave] = []
    notes: list[str] = []

    @property
    def ineffective(self) -> list[FaultWave]:
        """Waves that were attempted and found nothing to hit."""
        return [w for w in self.injected if not w.effective]


class HistoryStats(BaseModel):
    """`history_stats.json`: Jepsen record counts for one attack's history."""

    model_config = ConfigDict(extra="allow")

    total: int = 0
    ok: int = 0


def load(model: type[ArtifactT], path: Path) -> ArtifactT | None:
    """Read one artifact, or `None` if it is absent or unreadable.

    A caller cannot tell those apart and must not: both mean the run left no
    usable evidence here, which is an infrastructure error either way.
    """
    if not path.exists():
        return None
    try:
        return model.model_validate(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError, ValidationError):
        return None
