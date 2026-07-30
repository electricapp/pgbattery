"""Operation records and the thread-safe history they accumulate."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class Op:
    """A single client operation against the register.

    Fields:
        op_id:      monotonically increasing identifier, unique across workers.
        key:        which register this op targets.
        kind:       "read" | "write" | "cas".
        write_val:  for write/cas, the new value.
        cas_old:    for cas, the witness value.
        invoke_ts:  time.monotonic() right before the SQL is sent.
        return_ts:  time.monotonic() right after the SQL completes (or fails).
                    None ⇒ pending (no response ever received).
        result:     for read, the returned value; for write, the written value
                    on success; for cas, True iff the CAS committed. None ⇒ pending
                    or hard error (treated like pending in WGL).
        worker:     human-readable thread label.
        port:       gateway port the op was sent through.
    """

    op_id: int
    key: int
    kind: str
    invoke_ts: float
    return_ts: float | None = None
    write_val: int | None = None
    cas_old: int | None = None
    result: int | bool | None = None
    worker: str = ""
    port: int = 0
    # For kind="txn" / "append": ordered list of (mop_kind, key, val) micro-ops.
    # mop_kind ∈ {"r", "w", "append"}.
    # val is:
    #   - int   for register reads and writes
    #   - list[int] for list-append reads (the full observed list)
    #   - int   for list-append micro-ops (the single element being appended)
    #   - None  for pending or unobserved reads
    micro_ops: list[tuple[str, int, object]] = field(default_factory=list)

    @property
    def is_completed(self) -> bool:
        return self.return_ts is not None

    def to_jsonable(self) -> dict[str, object]:
        return {
            "op_id": self.op_id,
            "key": self.key,
            "kind": self.kind,
            "invoke_ts": round(self.invoke_ts, 6),
            "return_ts": round(self.return_ts, 6) if self.return_ts is not None else None,
            "write_val": self.write_val,
            "cas_old": self.cas_old,
            "result": self.result,
            "worker": self.worker,
            "port": self.port,
            "micro_ops": [list(m) for m in self.micro_ops] if self.micro_ops else [],
        }


@dataclass
class JepsenRecord:
    """One operation record in Jepsen / Elle history format.

    A single transaction produces two records: an `invoke` at start time
    and exactly one close (`ok` / `fail` / `info`) at the moment the worker
    learns the outcome. The format is what Elle's `check` consumes directly
    after wrapping in `jepsen.history/history` -- no Python-side
    reconstruction. Required fields and types described below.

    Fields:
        type:     "invoke" | "ok" | "fail" | "info"
        process:  worker id (single-threaded actor: at most one in-flight op)
        time_ns:  monotonic clock in integer nanoseconds, must be strictly
                  monotonic per process (Elle / jepsen.history asserts this)
        f:        function name, always "txn" for our workloads
        value:    list of micro-ops [[kind, key, val], ...] where
                  kind in {"r", "w", "append"}.
    """

    type: str
    process: int
    time_ns: int
    f: str
    value: list[list[object]]

    def to_jsonable(self) -> dict[str, object]:
        return {
            "type": self.type,
            "process": self.process,
            "time": self.time_ns,
            "f": self.f,
            "value": self.value,
        }


@dataclass
class History:
    """Thread-safe operation log.

    `ops` holds per-key register workload Ops (for WGL / weak checks).
    `jepsen` holds Jepsen-format records for the txn / list-append workloads
    (consumed by Elle directly).
    """

    ops: list[Op] = field(default_factory=list)
    jepsen: list[JepsenRecord] = field(default_factory=list)
    gateway_switches: int = 0
    _counter: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def next_id(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def record_gateway_switch(self) -> None:
        """Count a worker rebinding to a different gateway.

        Surfaced in the run summary: a high count means the workload spent time
        chasing the leader rather than committing, which is the difference
        between a dense history and one dominated by connection noise.
        """
        with self._lock:
            self.gateway_switches += 1

    def append(self, op: Op) -> None:
        with self._lock:
            self.ops.append(op)

    def append_jepsen(self, record: JepsenRecord) -> None:
        """Per-process monotonicity is the caller's responsibility (use
        `time.monotonic_ns()` and don't reorder)."""
        with self._lock:
            self.jepsen.append(record)

    def per_key(self, num_keys: int) -> dict[int, list[Op]]:
        out: dict[int, list[Op]] = {k: [] for k in range(num_keys)}
        for op in self.ops:
            out.setdefault(op.key, []).append(op)
        return out
