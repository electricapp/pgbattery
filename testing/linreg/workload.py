"""Table setup, single-op helpers, and the three worker loops.

Every loop takes its `WorkloadConfig` explicitly; nothing here reads a module
global, which is what makes living outside the entrypoint safe.
"""

from __future__ import annotations

import contextlib
import random
import re
import threading
import time

from db_clients import PsycopgWorkerClient
from linreg.cluster import GATEWAY_PORTS, run_cmd
from linreg.config import (
    GATEWAY_RETRY_BACKOFF_BASE,
    GATEWAY_RETRY_BACKOFF_MAX,
    PSQL_TIMEOUT_SECONDS,
    WorkloadConfig,
)
from linreg.records import History, JepsenRecord, Op


def next_gateway_port(current: int) -> int:
    """Next gateway port to try after a connection-level failure.

    Every gateway proxies to the current leader, so any live one will do; the
    dead one is the node whose container just died. Workers that stay pinned to
    it spin on connection-refused for the rest of the run — measured at ~2800
    attempts/second, which produced 56,600 of 56,665 `:info` records in one CI
    run while two surviving workers committed every real transaction.
    """
    idx = GATEWAY_PORTS.index(current) if current in GATEWAY_PORTS else -1
    return GATEWAY_PORTS[(idx + 1) % len(GATEWAY_PORTS)]


def rebind_after_indeterminate(
    client: PsycopgWorkerClient,
    history: History,
    consecutive: int,
) -> None:
    """Rotate `client` to another gateway and back off, after an `:info`.

    Called only once the op has been recorded, so recovery never changes what
    the history says. Backoff is capped so a fully-down cluster does not spin
    either, and is bounded well under the workload duration so a recovered
    cluster is picked up promptly.
    """
    history.record_gateway_switch()
    client.switch_port(next_gateway_port(client.port))
    time.sleep(min(GATEWAY_RETRY_BACKOFF_BASE * consecutive, GATEWAY_RETRY_BACKOFF_MAX))


def setup_table(num_keys: int) -> bool:
    """Create the linreg table seeded with key in [0, num_keys), val = 0.

    Returns True iff the table exists with num_keys rows post-setup.
    """
    setup_sql = (
        "DROP TABLE IF EXISTS linreg; "
        "CREATE TABLE linreg (key INTEGER PRIMARY KEY, val INTEGER NOT NULL); "
        f"INSERT INTO linreg SELECT generate_series(0, {num_keys - 1}), 0;"
    )
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            return True
    return False


def setup_list_append_table(num_keys: int) -> bool:
    """Create the linappend table for the list-append workload.

    Each row's val is a comma-separated decimal int list, starting empty.
    """
    setup_sql = (
        "DROP TABLE IF EXISTS linappend; "
        "CREATE TABLE linappend (key INTEGER PRIMARY KEY, val TEXT NOT NULL DEFAULT ''); "
        f"INSERT INTO linappend SELECT generate_series(0, {num_keys - 1}), '';"
    )
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            return True
    return False


_VAL_RE = re.compile(r"-?\d+")


def _parse_first_int(s: str) -> int | None:
    """Pull the first integer out of psql's -t -A output."""
    for line in s.strip().splitlines():
        match = _VAL_RE.match(line.strip())
        if match:
            with contextlib.suppress(ValueError):
                return int(match.group())
    return None


def do_read(port: int, key: int) -> tuple[int | None, bool]:
    """Execute a read; return (value, completed).

    completed = True iff we got a definite answer (success or definite reject).
    A timeout/conn-closed returns (None, False) → "pending".
    """
    cmd = (
        f"psql -h localhost -p {port} -U postgres -t -A -v ON_ERROR_STOP=1 "
        f"-c 'SELECT val FROM linreg WHERE key = {key}' 2>&1"
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return _parse_first_int(out), True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return None, True  # definite reject
    return None, False  # pending


def do_write(port: int, key: int, val: int) -> bool | None:
    """Execute a write; return True (acked), False (definite reject), None (pending)."""
    cmd = (
        f"psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 "
        f'-c "UPDATE linreg SET val = {val} WHERE key = {key}" 2>&1'
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return False  # definite reject
    return None  # pending


def do_cas(port: int, key: int, old: int, new: int) -> bool | None:
    """Execute a CAS; True on commit, False on witness-mismatch / reject, None pending."""
    cmd = (
        f"psql -h localhost -p {port} -U postgres -t -A -v ON_ERROR_STOP=1 "
        f'-c "UPDATE linreg SET val = {new} WHERE key = {key} AND val = {old} '
        f'RETURNING 1" 2>&1'
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return "1" in out  # one matching row updated → True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return False
    return None


def txn_worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
    cfg: WorkloadConfig,
) -> None:
    """2-key SERIALIZABLE rw-register transactions, emitting Jepsen-format
    records directly.

    Per Jepsen: a worker is a single-threaded actor that issues one txn at
    a time. We emit `:invoke` when sending and exactly one of `:ok`/`:fail`/
    `:info` when the outcome is known (success / definite rollback / unknown).
    """
    port = GATEWAY_PORTS[worker_id % len(GATEWAY_PORTS)]
    client = PsycopgWorkerClient(port=port)
    consecutive_indeterminate = 0
    try:
        while not stop_event.is_set():
            if cfg.keys < 2:
                return
            k1, k2 = rng.sample(range(cfg.keys), 2)
            # Write values come from the global counter so every version of
            # every key is distinct. Elle infers rw-register version order
            # from which value a read observed; two writes of the same value
            # to one key make that inference ambiguous, which costs real
            # dependency edges and turns anomalies into `:unknown`. The
            # counter starts at 1, so no write collides with the seeded 0.
            new1 = history.next_id()
            new2 = history.next_id()
            # Invoke: reads pending, writes declared.
            invoke_value: list[list[object]] = [
                ["r", k1, None],
                ["r", k2, None],
                ["w", k1, new1],
                ["w", k2, new2],
            ]
            history.append_jepsen(
                JepsenRecord(
                    type="invoke",
                    process=worker_id,
                    time_ns=time.monotonic_ns(),
                    f="txn",
                    value=invoke_value,
                )
            )
            outcome = client.execute_register_txn(k1, k2, new1, new2)
            close_ns = time.monotonic_ns()
            if outcome.committed is True:
                r1, r2 = outcome.reads[0], outcome.reads[1]
                ok_value: list[list[object]] = [
                    ["r", k1, r1 if isinstance(r1, int) else None],
                    ["r", k2, r2 if isinstance(r2, int) else None],
                    ["w", k1, new1],
                    ["w", k2, new2],
                ]
                history.append_jepsen(
                    JepsenRecord(
                        type="ok",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=ok_value,
                    )
                )
                consecutive_indeterminate = 0
            elif outcome.committed is False:
                # Definite rollback: txn had no effect. Same value as invoke.
                history.append_jepsen(
                    JepsenRecord(
                        type="fail",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
                # A definite rollback means the gateway answered, so the
                # connection is healthy; only indeterminate outcomes indicate a
                # dead gateway.
                consecutive_indeterminate = 0
            else:
                # Pending: connection broke / timed out. We don't know if the
                # cluster committed, so the txn closes as :info. An :info op
                # is never "completed" for ordering purposes: its effect
                # window is [invoke, end-of-history], not [invoke, info], and
                # it contributes no outgoing realtime edge. The `time` on the
                # record is only when we gave up waiting.
                #
                # Process reuse: Jepsen's generator retires the process of a
                # crashed (:info) op and remaps the thread to a fresh process
                # number, exactly because that process may still be in flight
                # forever. `jepsen.history` does no such thing, and neither do
                # we — both worker loops keep emitting under the same
                # `process=worker_id` after an :info. That is sound for the
                # only model the shim checks, :strict-serializable (see
                # elle_shim/src/pgbattery_elle_shim/core.clj): the :info op
                # takes no realtime completion edge, and process order is not
                # one of that model's graphs. It would be UNSOUND for
                # :sequential or any other process-order model, which derives
                # edges between consecutive ops of one process and would order
                # the possibly-still-running :info op before every later op of
                # the reused id. Adding such a model requires retiring the
                # process id here first.
                history.append_jepsen(
                    JepsenRecord(
                        type="info",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
                # Recover only after the op is recorded, so chasing the leader
                # never changes what the history says happened.
                consecutive_indeterminate += 1
                rebind_after_indeterminate(client, history, consecutive_indeterminate)
    finally:
        client.close()


def list_append_worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
    cfg: WorkloadConfig,
) -> None:
    """2-key SERIALIZABLE list-append transactions, emitting Jepsen records
    directly. Each txn appends a globally-unique tag (the worker's local
    counter combined with worker_id) to both keys."""
    port = GATEWAY_PORTS[worker_id % len(GATEWAY_PORTS)]
    client = PsycopgWorkerClient(port=port)
    consecutive_indeterminate = 0
    try:
        while not stop_event.is_set():
            if cfg.keys < 2:
                return
            k1, k2 = rng.sample(range(cfg.keys), 2)
            tag = history.next_id()
            invoke_value: list[list[object]] = [
                ["r", k1, None],
                ["r", k2, None],
                ["append", k1, tag],
                ["append", k2, tag],
            ]
            history.append_jepsen(
                JepsenRecord(
                    type="invoke",
                    process=worker_id,
                    time_ns=time.monotonic_ns(),
                    f="txn",
                    value=invoke_value,
                )
            )
            outcome = client.execute_append_txn(k1, k2, tag)
            close_ns = time.monotonic_ns()
            if outcome.committed is True:
                l1, l2 = outcome.reads[0], outcome.reads[1]
                ok_value: list[list[object]] = [
                    ["r", k1, l1 if isinstance(l1, list) else None],
                    ["r", k2, l2 if isinstance(l2, list) else None],
                    ["append", k1, tag],
                    ["append", k2, tag],
                ]
                history.append_jepsen(
                    JepsenRecord(
                        type="ok",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=ok_value,
                    )
                )
                consecutive_indeterminate = 0
            elif outcome.committed is False:
                history.append_jepsen(
                    JepsenRecord(
                        type="fail",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
                # A definite rollback means the gateway answered, so the
                # connection is healthy; only indeterminate outcomes indicate a
                # dead gateway.
                consecutive_indeterminate = 0
            else:
                history.append_jepsen(
                    JepsenRecord(
                        type="info",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
                # Recover only after the op is recorded, so chasing the leader
                # never changes what the history says happened.
                consecutive_indeterminate += 1
                rebind_after_indeterminate(client, history, consecutive_indeterminate)
    finally:
        client.close()


def worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
    cfg: WorkloadConfig,
) -> None:
    """Issue ops at high rate until stop_event is set.

    The op mix is read 50% / write 30% / cas 20% — enough writes to create
    real ordering history, enough reads to make ordering observable, and
    a CAS workload that surfaces lost-update style anomalies.
    """
    worker_label = f"w{worker_id}"
    # Each worker rotates through gateway ports so we exercise routing during
    # failover. The leader port routes; followers reject.
    port_cycle_index = 0
    while not stop_event.is_set():
        port = GATEWAY_PORTS[port_cycle_index % len(GATEWAY_PORTS)]
        port_cycle_index += 1
        key = rng.randrange(cfg.keys)
        choice = rng.random()
        op = Op(
            op_id=history.next_id(),
            key=key,
            kind="?",
            invoke_ts=time.monotonic(),
            worker=worker_label,
            port=port,
        )
        if choice < 0.50:
            op.kind = "read"
            value, completed = do_read(port, key)
            op.return_ts = time.monotonic() if completed else None
            op.result = value
        elif choice < 0.80:
            new_val = rng.randint(1, 1_000_000)
            op.kind = "write"
            op.write_val = new_val
            outcome = do_write(port, key, new_val)
            if outcome is None:
                op.return_ts = None
            else:
                op.return_ts = time.monotonic()
                op.result = outcome
        else:
            old_val = rng.randint(0, 1_000_000)
            new_val = rng.randint(1, 1_000_000)
            op.kind = "cas"
            op.cas_old = old_val
            op.write_val = new_val
            outcome = do_cas(port, key, old_val, new_val)
            if outcome is None:
                op.return_ts = None
            else:
                op.return_ts = time.monotonic()
                op.result = outcome
        history.append(op)
