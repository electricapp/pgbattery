#!/usr/bin/env -S uv run --project testing python
"""Proof that the mid-upgrade chaos holds the right process and reads the right answers.

`pg-major-upgrade-chaos-mid-upgrade` used to `pgrep -f pg_upgrade`, which also
matches the preflight's `pg_upgrade --check` and the scratch cluster it builds,
so the SIGKILL could land outside the apply and the case passed while proving
nothing. The shim replaces the guess: only an apply's pg_upgrade is held, and
it becomes the real binary under the pid the harness was told about.

These cases run the shim scripts against fake binaries and pin the files that
must agree with each other: the configs only the chaos case mounts, the
override that mounts them, and the harness's own constants.

Run with:
    uv run --project testing python testing/test_pg_upgrade_shim.py
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from typing import Any, Final

import yaml

import pg_major_upgrade_proof as proof
import topology

SHIM_DIR: Final[Path] = topology.REPO_ROOT / "testing/pg_upgrade_shim"
WRAPPER: Final[Path] = SHIM_DIR / "pg_upgrade"
INSTALL: Final[Path] = SHIM_DIR / "install.sh"
SHIM_FILES: Final[list[str]] = [proof.COMPOSE_FILE, proof.SHIM_COMPOSE_FILE]


def executable(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def await_file(path: Path, budget: float = 5.0) -> str:
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        if path.exists() and path.read_text(encoding="utf-8").strip():
            return path.read_text(encoding="utf-8").strip()
        time.sleep(0.01)
    raise AssertionError(f"{path} never appeared")


class WrapperHoldsOnlyTheApply(unittest.TestCase):
    """The wrapper against a fake real binary that records its pid and argv."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="pg-upgrade-shim-"))
        self.bindir = self.root / "bin"
        self.bindir.mkdir()
        (self.bindir / "pg_upgrade").symlink_to(WRAPPER)
        self.out = self.root / "real-ran"
        executable(self.bindir / "pg_upgrade.real", f'echo "$$ $*" > "{self.out}"')
        self.hold = self.root / "hold"
        self.env = {**os.environ, "PG_UPGRADE_SHIM_HOLD": str(self.hold)}

    def test_check_runs_straight_through(self) -> None:
        subprocess.run(
            [self.bindir / "pg_upgrade", "--old-datadir", "d", "--check", "--copy"],
            env=self.env,
            check=True,
            timeout=5,
        )
        self.assertIn("--check", self.out.read_text(encoding="utf-8"))
        self.assertFalse((self.hold / "held.pid").exists())

    def test_apply_waits_for_release_then_execs_under_the_same_pid(self) -> None:
        held = subprocess.Popen(
            [self.bindir / "pg_upgrade", "--old-datadir", "d", "--copy"], env=self.env
        )
        try:
            pid = await_file(self.hold / "held.pid")
            self.assertEqual(int(pid), held.pid)
            time.sleep(0.2)
            self.assertIsNone(held.poll(), "the apply ran without being released")
            self.assertFalse(self.out.exists())
            (self.hold / "release").touch()
            self.assertEqual(held.wait(timeout=5), 0)
        finally:
            held.kill()
        self.assertEqual(
            self.out.read_text(encoding="utf-8").split(), [pid, "--old-datadir", "d", "--copy"]
        )

    def test_default_hold_dir_is_the_one_the_harness_watches(self) -> None:
        self.assertIn(f"${{PG_UPGRADE_SHIM_HOLD:-{proof.SHIM_HOLD_DIR}}}", WRAPPER.read_text())


class InstallBuildsTheShimBindir(unittest.TestCase):
    def test_every_binary_is_real_except_pg_upgrade(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="pg-upgrade-install-"))
        real = root / "real"
        real.mkdir()
        for name in ("postgres", "pg_ctl", "initdb", "pg_upgrade"):
            executable(real / name, "exit 0")
        shim = root / "shim"
        subprocess.run(["sh", INSTALL, real, shim], check=True, timeout=10)
        self.assertEqual(os.readlink(shim / "postgres"), str(real / "postgres"))
        self.assertEqual(os.readlink(shim / "initdb"), str(real / "initdb"))
        self.assertEqual(os.readlink(shim / "pg_upgrade.real"), str(real / "pg_upgrade"))
        self.assertEqual(os.readlink(shim / "pg_upgrade"), str(WRAPPER))

    def test_refuses_a_bindir_without_pg_upgrade(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="pg-upgrade-install-"))
        (root / "real").mkdir()
        done = subprocess.run(["sh", INSTALL, root / "real", root / "shim"], timeout=10)
        self.assertNotEqual(done.returncode, 0)


def config(name: str) -> dict[str, Any]:
    return tomllib.loads((topology.REPO_ROOT / name).read_text(encoding="utf-8"))


class OnlyTheChaosCaseSeesTheShim(unittest.TestCase):
    """The override's configs are the base configs with one key changed."""

    def test_shim_configs_differ_from_base_only_in_the_target_bindir(self) -> None:
        base = topology.load(topology.REPO_ROOT / proof.COMPOSE_FILE)
        for node in base.nodes:
            shim = proof.node_paths(node.service, SHIM_FILES)
            plain = proof.node_paths(node.service, [proof.COMPOSE_FILE])
            self.assertEqual(shim.pg_data_dir, plain.pg_data_dir)
            self.assertNotEqual(shim.pg_bin_dirs["18"], plain.pg_bin_dirs["18"])
            self.assertEqual(shim.pg_bin_dirs["17"], plain.pg_bin_dirs["17"])

            whole_shim = config(f"config/pgupgrade-shim/{node.service}.toml")
            whole_base = config(f"config/pgupgrade/{node.service}.toml")
            whole_shim["pg_bin_dirs"].pop("18")
            whole_base["pg_bin_dirs"].pop("18")
            self.assertEqual(whole_shim, whole_base, node.service)

    def test_the_override_mounts_the_shim_where_the_harness_installs_from(self) -> None:
        override = yaml.safe_load(
            (topology.REPO_ROOT / proof.SHIM_COMPOSE_FILE).read_text(encoding="utf-8")
        )
        base = topology.load(topology.REPO_ROOT / proof.COMPOSE_FILE)
        self.assertEqual(set(override["services"]), {n.service for n in base.nodes})
        for spec in override["services"].values():
            self.assertIn(f"./testing/pg_upgrade_shim:{proof.SHIM_MOUNT}:ro", spec["volumes"])

    def test_the_plain_proof_runs_the_real_binaries(self) -> None:
        self.assertEqual(proof.compose_files(None), [proof.COMPOSE_FILE])
        self.assertEqual(proof.compose_files(proof.MID_UPGRADE), SHIM_FILES)


class OracleReadings(unittest.TestCase):
    def test_kill_report_parses(self) -> None:
        report = proof.parse_kill_report(
            "noise\nkilled pid=42 exe=/usr/lib/postgresql/18/bin/pg_upgrade servers=none\n"
        )
        self.assertEqual(report.pid, 42)
        self.assertEqual(report.servers, "none")

    def test_a_kill_that_reported_nothing_is_a_failure(self) -> None:
        for output in ("", "pid 9 exited before becoming /x", "killed pid=x exe=/y servers=none"):
            with self.assertRaises(proof.ContractFailure):
                proof.parse_kill_report(output)

    def test_freeze_verdict(self) -> None:
        self.assertTrue(proof.freeze_verdict(False, None, 3.0, 4.0))
        self.assertFalse(proof.freeze_verdict(True, None, 3.0, 3.0))

    def test_freeze_verdict_refuses_what_is_neither(self) -> None:
        for answered, error, after in (
            (False, None, 3.0),  # silent and not parked: a dead gateway, not a freeze
            (True, "connection refused", 3.0),
            (True, None, 4.0),  # parked, then answered: the freeze lifted mid-probe
        ):
            with self.assertRaises(proof.ContractFailure):
                proof.freeze_verdict(answered, error, 3.0, after)

    def test_target_datadir_follows_the_product_convention(self) -> None:
        self.assertEqual(
            proof.target_datadir("/var/lib/postgresql/data", 18), "/var/lib/postgresql/data_18"
        )
        self.assertEqual(proof.target_datadir("/srv/pg/data_17", 18), "/srv/pg/data_18")


if __name__ == "__main__":
    unittest.main()
