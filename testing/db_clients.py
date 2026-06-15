"""Persistent-connection psycopg clients for the high-throughput workloads.

Each worker thread owns one `PsycopgWorkerClient`. The client:

  - Opens a single connection to a gateway port on first use.
  - Reuses that connection for every transaction.
  - On `OperationalError` / `InterfaceError` (gateway disconnects during
    failover, server forcibly closes the conn, etc.) the client transparently
    closes the broken handle and the *next* call re-opens against the same
    port. The op that hit the error is reported as `pending` so Elle records
    it as `:info` (outcome indeterminate).

Why no connection pool?

  - Single conn per worker is exactly what we want: when the gateway drops
    the conn during a leader migration, the worker sees it on its next op
    and we can record the migration boundary cleanly. A pool would mask it
    behind an opaque retry.
  - Pools add latency and a separate failure mode (pool exhaustion) we
    don't need.

Why psycopg over `psql -c`?

  - `psql` forks a process and opens a fresh TCP+TLS+auth+startup-message
    handshake on every op. ~25-35 ms of pure overhead per transaction.
  - psycopg holds the conn for the lifetime of the worker. Per-op latency
    is the round-trip to PG plus the actual SQL: ~1-3 ms.
  - On a 4-worker / 30 s run this lifts aggregate throughput from ~120
    ops/s to ~1500-2000 ops/s, which is the difference between Elle
    catching nothing on a sparse graph and Elle exploring a real one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import psycopg

# Connection timeouts. We want the worker to notice a hung gateway quickly
# during failover rather than blocking the entire 30s budget on one op.
CONNECT_TIMEOUT_S: Final[int] = 3
STATEMENT_TIMEOUT_MS: Final[int] = 3_000


@dataclass
class TxnOutcome:
    """Result of a multi-statement transaction attempt.

    Fields:
        committed: True if COMMIT succeeded; False on definite ROLLBACK
            (serialization failure, read-only error); None if the connection
            died mid-tx (treat as pending → Elle :info).
        reads:     Per-statement read values, in the order they were issued.
                   For list-append: each entry is a list[int]. For rw-register:
                   each entry is an int.
                   Empty on a pending/rolled-back tx.
    """

    committed: bool | None
    reads: list[object]


class PsycopgWorkerClient:
    """Owns one psycopg connection bound to a single gateway port.

    Not thread-safe — each worker thread holds its own instance.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5432,
        user: str = "postgres",
        dbname: str = "postgres",
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.dbname = dbname
        self._conn: psycopg.Connection | None = None

    # ── Lifecycle ───────────────────────────────────────────────────────────

    def _connect(self) -> psycopg.Connection | None:
        """Open a new connection; return None on connect failure.

        Sets `autocommit = False` so we can drive explicit BEGIN/COMMIT.
        Applies `statement_timeout` so a hung query doesn't pin the worker
        for the rest of the run.
        """
        try:
            conn = psycopg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                dbname=self.dbname,
                connect_timeout=CONNECT_TIMEOUT_S,
                autocommit=False,
            )
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
            conn.commit()
            return conn
        except (psycopg.OperationalError, psycopg.InterfaceError, OSError):
            return None

    def _ensure_conn(self) -> psycopg.Connection | None:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            with _suppress():
                self._conn.close()
        self._conn = None

    def _reset_after_failure(self) -> None:
        """Force the next op to re-open. Used after any conn-level error."""
        if self._conn is not None:
            with _suppress():
                self._conn.close()
        self._conn = None

    def switch_port(self, port: int) -> None:
        """Rebind to a different gateway port (e.g. when chasing the leader).

        The current connection is closed; the next op opens against `port`.
        """
        if port != self.port:
            self._reset_after_failure()
            self.port = port

    # ── rw-register txn (2 keys) ────────────────────────────────────────────

    def execute_register_txn(
        self,
        k1: int,
        k2: int,
        new1: int,
        new2: int,
    ) -> TxnOutcome:
        """Run a 2-key SERIALIZABLE register transaction.

        Reads both keys, writes new values to both, commits. Returns the
        observed read values on success.
        """
        conn = self._ensure_conn()
        if conn is None:
            return TxnOutcome(committed=None, reads=[])
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                cur.execute("SELECT val FROM linreg WHERE key = %s", (k1,))
                r1_row = cur.fetchone()
                cur.execute("SELECT val FROM linreg WHERE key = %s", (k2,))
                r2_row = cur.fetchone()
                cur.execute("UPDATE linreg SET val = %s WHERE key = %s", (new1, k1))
                cur.execute("UPDATE linreg SET val = %s WHERE key = %s", (new2, k2))
                conn.commit()
            r1 = r1_row[0] if r1_row else None
            r2 = r2_row[0] if r2_row else None
            return TxnOutcome(committed=True, reads=[r1, r2])
        except (psycopg.errors.SerializationFailure, psycopg.errors.ReadOnlySqlTransaction):
            with _suppress():
                conn.rollback()
            return TxnOutcome(committed=False, reads=[])
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._reset_after_failure()
            return TxnOutcome(committed=None, reads=[])
        except psycopg.Error:
            # Anything else (e.g. statement_timeout, deadlock) → pending.
            self._reset_after_failure()
            return TxnOutcome(committed=None, reads=[])

    # ── list-append txn (2 keys) ────────────────────────────────────────────

    def execute_append_txn(
        self,
        k1: int,
        k2: int,
        tag: int,
    ) -> TxnOutcome:
        """Run a 2-key SERIALIZABLE list-append transaction.

        Reads both keys' lists, appends `tag` to both, commits. Returns the
        observed list values on success (as Python lists of ints).
        """
        conn = self._ensure_conn()
        if conn is None:
            return TxnOutcome(committed=None, reads=[])
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                cur.execute("SELECT val FROM linappend WHERE key = %s", (k1,))
                r1_row = cur.fetchone()
                cur.execute("SELECT val FROM linappend WHERE key = %s", (k2,))
                r2_row = cur.fetchone()
                # Append `tag` as a decimal int to a comma-separated list.
                # Empty string becomes "tag"; otherwise "...,tag".
                append_sql = (
                    "UPDATE linappend "
                    "SET val = CASE WHEN val = '' THEN %s::text "
                    "ELSE val || ',' || %s::text END "
                    "WHERE key = %s"
                )
                cur.execute(append_sql, (tag, tag, k1))
                cur.execute(append_sql, (tag, tag, k2))
                conn.commit()
            r1 = _parse_list(r1_row[0] if r1_row else "")
            r2 = _parse_list(r2_row[0] if r2_row else "")
            return TxnOutcome(committed=True, reads=[r1, r2])
        except (psycopg.errors.SerializationFailure, psycopg.errors.ReadOnlySqlTransaction):
            with _suppress():
                conn.rollback()
            return TxnOutcome(committed=False, reads=[])
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._reset_after_failure()
            return TxnOutcome(committed=None, reads=[])
        except psycopg.Error:
            self._reset_after_failure()
            return TxnOutcome(committed=None, reads=[])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _parse_list(s: str) -> list[int]:
    """Parse a comma-separated int list. Empty string -> []."""
    if not s:
        return []
    out: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            # Garbage in the cell — surface as-is would crash Elle's
            # adapter; treat as no observation. Caller will see a
            # shorter list, which Elle will detect as a missing element.
            continue
    return out


class _suppress:
    """tiny contextmanager that swallows everything; used in cleanup paths."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_: object) -> bool:
        return True


__all__ = ["PsycopgWorkerClient", "TxnOutcome"]
