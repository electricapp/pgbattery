#!/usr/bin/env -S uv run --project testing python
"""Proof that every CLI flag reaches the code it configures.

`linearizability_register.py` used to reconfigure its workload by rebinding
module globals from `run()`. That works only while every consumer lives in the
same module, because `global` rebinds a name in the module that defines it and
nowhere else. Splitting the file would have left workers and checkers reading
the module defaults while the harness reported on the requested configuration —
a silently weakened workload, which no output distinguishes from a real run.

These tests are the guard that makes the split safe. They assert the config is
threaded explicitly, that no module-level config global remains for a future
module to read by accident, and that `--keys` measurably governs the workers and
the table setup.

`--workers`, `--duration`, and `--fault-at` are consumed only inside `run()`,
which needs a live cluster, so they are covered structurally: the CLI default is
the module constant, and the frozen config carries the value. `--keys` is
covered behaviourally because it reaches workers, setup, and checkers.

No cluster, no Docker, no Elle uberjar. Stdlib `unittest`, matching
test_checker_sanity.py.

Run with:
    uv run --project testing python testing/test_workload_config.py
"""

from __future__ import annotations

import random
import threading
import unittest
from dataclasses import FrozenInstanceError, fields
from inspect import signature
from pathlib import Path
from typing import Final
from unittest import mock

import linearizability_register as lr
from linreg import workload
from linreg.config import WorkloadConfig
from linreg.records import History

RETIRED_GLOBALS: Final[tuple[str, ...]] = (
    "NUM_WORKERS",
    "NUM_KEYS",
    "WORKLOAD_DURATION_SECONDS",
    "KILL_LEADER_AFTER_SECONDS",
)
"""The names `run()` used to rebind. Their absence is the point."""

CLI_FLAG_TO_DEFAULT: Final[dict[str, str]] = {
    "workers": "DEFAULT_NUM_WORKERS",
    "keys": "DEFAULT_NUM_KEYS",
    "duration": "DEFAULT_WORKLOAD_DURATION_SECONDS",
    "fault_at": "DEFAULT_KILL_LEADER_AFTER_SECONDS",
}

WORKER_LOOPS: Final[tuple[str, ...]] = (
    "worker_loop",
    "txn_worker_loop",
    "list_append_worker_loop",
)


class _StubClient:
    """Stands in for `PsycopgWorkerClient` so the txn loops open no sockets."""

    def __init__(self, port: int = 0) -> None:
        self.port = port

    def close(self) -> None:
        pass

    def switch_port(self, port: int) -> None:
        self.port = port


def drive_register_worker(cfg: WorkloadConfig, iterations: int, seed: int = 1) -> list[int]:
    """Run `worker_loop` with the op helpers stubbed; return the keys it touched.

    Stops the loop from inside the stub once `iterations` ops have been issued,
    so the worker exits on its own rather than on a timer.
    """
    keys_seen: list[int] = []
    stop = threading.Event()

    def record(key: int) -> None:
        keys_seen.append(key)
        if len(keys_seen) >= iterations:
            stop.set()

    def stub_read(port: int, key: int) -> tuple[int | None, bool]:
        record(key)
        return 0, True

    def stub_write(port: int, key: int, val: int) -> bool | None:
        record(key)
        return True

    def stub_cas(port: int, key: int, old: int, new: int) -> bool | None:
        record(key)
        return True

    with mock.patch.multiple(workload, do_read=stub_read, do_write=stub_write, do_cas=stub_cas):
        workload.worker_loop(0, History(), stop, random.Random(seed), cfg)
    return keys_seen


def captured_setup_sql(fn_name: str, num_keys: int) -> str:
    """Call a setup function with `run_cmd` stubbed; return the SQL it built."""
    seen: list[str] = []

    def stub_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
        seen.append(cmd)
        return 0, "", ""

    with mock.patch.object(workload, "run_cmd", stub_run_cmd):
        getattr(workload, fn_name)(num_keys)
    if not seen:
        raise AssertionError(f"{fn_name} issued no command")
    return seen[0]


class TestNoConfigGlobalsRemain(unittest.TestCase):
    """The direct guard: a module global is a stale value waiting for a split."""

    def test_retired_globals_are_gone(self) -> None:
        for name in RETIRED_GLOBALS:
            with self.subTest(name=name):
                self.assertFalse(
                    hasattr(lr, name),
                    f"{name} is a module global again; a consumer in another "
                    "module would read it instead of the CLI value",
                )

    def test_no_module_declares_a_global_statement(self) -> None:
        """Covers the package too, not just the entrypoint.

        A `global` inside `linreg/` is worse than one in the entrypoint: it
        rebinds only within its own module, so the CLI value never reaches it and
        the harness reports on a workload nobody asked for.
        """
        entrypoint = Path(lr.__file__)
        sources = [entrypoint, *sorted((entrypoint.parent / "linreg").glob("*.py"))]
        offenders: list[str] = []
        for source in sources:
            for n, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1):
                if line.lstrip().startswith("global "):
                    offenders.append(f"{source.name}:{n}: {line.strip()}")
        self.assertEqual(offenders, [], "`global` reappeared")
        # The scan is only meaningful if it actually found the package.
        self.assertGreater(len(sources), 1, "linreg package not found; scan was vacuous")


class TestConfigObject(unittest.TestCase):
    def test_config_is_frozen(self) -> None:
        cfg = WorkloadConfig()
        with self.assertRaises(FrozenInstanceError):
            cfg.keys = 99  # type: ignore[misc]

    def test_config_carries_non_defaults(self) -> None:
        cfg = WorkloadConfig(workers=9, keys=11, duration_s=13.5, fault_at=0.25)
        self.assertEqual((cfg.workers, cfg.keys), (9, 11))
        self.assertEqual((cfg.duration_s, cfg.fault_at), (13.5, 0.25))

    def test_every_cli_flag_has_a_config_field(self) -> None:
        """A flag with nowhere to land would be silently discarded."""
        field_names = {f.name for f in fields(WorkloadConfig)}
        self.assertEqual(field_names, {"workers", "keys", "duration_s", "fault_at"})

    def test_cli_defaults_are_the_module_constants(self) -> None:
        params = signature(lr.run).parameters
        for flag, const_name in CLI_FLAG_TO_DEFAULT.items():
            with self.subTest(flag=flag):
                option = params[flag].default
                self.assertEqual(
                    option.default,
                    getattr(lr, const_name),
                    f"--{flag} default drifted from {const_name}",
                )


class TestWorkerLoopsTakeConfig(unittest.TestCase):
    """`run()` spawns threads with a positional arg tuple. A signature mismatch
    raises TypeError inside a daemon thread, where it dies unseen and the run
    reports PASS on an empty history."""

    def test_thread_arg_tuple_binds_to_every_loop(self) -> None:
        args = (0, History(), threading.Event(), random.Random(0), WorkloadConfig())
        for name in WORKER_LOOPS:
            with self.subTest(loop=name):
                signature(getattr(workload, name)).bind(*args)

    def test_all_loops_share_one_parameter_list(self) -> None:
        """The workload dispatch picks a loop by name, so they must be
        interchangeable."""
        shapes = {
            name: list(signature(getattr(workload, name)).parameters) for name in WORKER_LOOPS
        }
        distinct = {tuple(v) for v in shapes.values()}
        self.assertEqual(len(distinct), 1, f"worker loops disagree on parameters: {shapes}")


class TestKeysGovernsBehaviour(unittest.TestCase):
    def test_single_key_confines_the_register_worker(self) -> None:
        keys_seen = drive_register_worker(WorkloadConfig(keys=1), iterations=40)
        self.assertEqual(set(keys_seen), {0}, "keys=1 must confine the worker to key 0")

    def test_wider_keyspace_is_actually_used(self) -> None:
        """Confinement alone would also pass if the key were hardcoded to 0."""
        keys_seen = drive_register_worker(WorkloadConfig(keys=7), iterations=200)
        self.assertTrue(all(0 <= k < 7 for k in keys_seen), f"key outside [0,7): {keys_seen}")
        self.assertGreater(
            len(set(keys_seen)), 1, "keys=7 produced a single key; cfg.keys is being ignored"
        )

    def test_keys_reaches_both_table_setups(self) -> None:
        for fn_name, table in (
            ("setup_table", "linreg"),
            ("setup_list_append_table", "linappend"),
        ):
            with self.subTest(fn=fn_name):
                sql = captured_setup_sql(fn_name, num_keys=5)
                self.assertIn(table, sql)
                self.assertIn("generate_series(0, 4)", sql)

    def test_txn_loops_refuse_a_single_key(self) -> None:
        """Both 2-key workloads need at least two keys; they must read that from
        the config, not from a default."""
        for name in ("txn_worker_loop", "list_append_worker_loop"):
            with self.subTest(loop=name):
                stop = threading.Event()
                with mock.patch.object(workload, "PsycopgWorkerClient", _StubClient):
                    getattr(lr, name)(0, History(), stop, random.Random(0), WorkloadConfig(keys=1))


class TestPerKeyTakesCountExplicitly(unittest.TestCase):
    def test_per_key_has_no_default_count(self) -> None:
        """A default would let a checker in another module bucket by the wrong
        keyspace and silently drop keys from the report."""
        param = signature(History.per_key).parameters["num_keys"]
        self.assertIs(param.default, param.empty)

    def test_per_key_buckets_every_requested_key(self) -> None:
        buckets = History().per_key(5)
        self.assertEqual(sorted(buckets), [0, 1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
