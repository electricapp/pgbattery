#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the Raft torn-write suite's verdict logic.

No docker. These cover what the suite concludes from an attempt it could not
fully observe, which is the part that decides whether a contract may be claimed.

Run with:
    uv run --project testing python testing/test_torn_raft.py
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import psycopg

import fault_primitives as fp
import harness_fakes as hf
import topology
import torn_raft as tr


def outcome(**kwargs: object) -> tr.Outcome:
    """An Outcome with the fields the verdict reads, defaults chosen to be silent."""
    result = tr.Outcome()
    for key, value in kwargs.items():
        setattr(result, key, value)
    return result


def membership(*voters: str, learners: tuple[str, ...] = ()) -> dict[str, object]:
    """A `/cluster/members` payload in the shape `list_members` serialises."""

    def entry(node: str, role: str) -> dict[str, object]:
        return {"node_id": int(node.removeprefix("node")), "addr": "", "role": role}

    members = [entry(node, "voter") for node in voters]
    members += [entry(node, "learner") for node in learners]
    return {"success": True, "message": f"{len(members)} members in cluster", "members": members}


def cluster_runner(
    *,
    leader: str | None = "node1",
    voters: tuple[str, ...] = ("node1", "node2", "node3"),
    container: str = "running",
    checkpoints: bool = True,
    torn_record: tuple[int, int] | None = None,
    silent: str | None = None,
    node_log: str = "",
    restarts: int = 0,
) -> ClusterRunner:
    """A shell that answers as a healthy cluster.

    Rules are matched in order, so the specific ones come first. Driving the
    suite through this rather than patching its functions means the test
    exercises the real call path — the curl, the docker inspect, the FIFO
    write and the log read — and stays true when that path is refactored.
    """
    leader_json = f'{{"leader_id":{leader.removeprefix("node")}}}' if leader else "{}"
    raft_log = "[lazyfs]: using a custom config\n"
    if torn_record is not None:
        persisted, offset = torn_record
        raft_log += (
            f"[lazyfs.faults]: Write to path {tr.RAFT_DB}: "
            f"will persist {persisted} bytes from offset {offset}\n"
        )
    return ClusterRunner(
        rules=[
            ("/api/v1/cluster/leader", hf.ok(leader_json)),
            ("/api/v1/cluster/members", hf.ok(json.dumps(membership(*voters)))),
            ("docker compose ps -aq", hf.ok("container-id" if container else "")),
            (
                "docker inspect",
                hf.ok(f"{container} 2026-01-01T00:00:00Z {restarts}" if container else ""),
            ),
            (fp.LAZYFS_RAFT.log, hf.ok(raft_log)),
            ("docker compose logs", hf.ok(node_log)),
        ],
        checkpoints=checkpoints,
        silent=silent,
    )


class ClusterRunner(hf.ScriptedRunner):
    """A scripted cluster whose LazyFS log grows when a checkpoint is asked for.

    `flush_lazyfs_cache` waits for the count of applied checkpoints to *rise*,
    not merely to be non-zero, because a log that already carried one would
    otherwise let a flush that never happened read as done. A static log would
    make that wait un-satisfiable, so the double has to model the growth.
    """

    def __init__(
        self,
        rules: list[tuple[str, fp.CommandResult]],
        *,
        checkpoints: bool = True,
        silent: str | None = None,
    ) -> None:
        super().__init__(rules)
        self.checkpoints = checkpoints
        self.silent_port = f":{topology.MGMT_PORTS[silent]}/" if silent else None
        self.applied = 0

    def __call__(self, cmd: str, timeout_s: float) -> fp.CommandResult:
        if self.checkpoints and fp.lazyfs_checkpoint_cmd() in cmd:
            self.applied += 1
        if self.silent_port is not None and self.silent_port in cmd:
            self.calls.append(cmd)
            return hf.fail("curl: (7) Failed to connect")
        if fp.LAZYFS_DATA.log in cmd:
            self.calls.append(cmd)
            return hf.ok(f"[lazyfs.cmds]: {fp.LAZYFS_CHECKPOINT_DONE}\n" * self.applied)
        return super().__call__(cmd, timeout_s)


class TransferRunner(hf.ScriptedRunner):
    """A cluster whose leadership moves when, and only when, it is asked to.

    Causal rather than counted: the answer changes because the transfer was
    requested, not because a number of polls went by. A round-counting double
    passes when the code polls the expected number of times, which is the
    coupling this whole rewrite is removing.
    """

    def __init__(self, start: str, target: str) -> None:
        super().__init__(
            [("/api/v1/cluster/members", hf.ok(json.dumps(membership(*topology.NODES))))]
        )
        self.leader = start
        self.target = target

    def __call__(self, cmd: str, timeout_s: float) -> fp.CommandResult:
        if "transfer-leadership" in cmd:
            self.calls.append(cmd)
            self.leader = self.target
            return hf.ok()
        if "/api/v1/cluster/leader" in cmd:
            self.calls.append(cmd)
            return hf.ok(f'{{"leader_id":{self.leader.removeprefix("node")}}}')
        return super().__call__(cmd, timeout_s)


class SeamTest(hf.HarnessFixture):
    """Drives the suite through its two seams: the shell and the database.

    Every wait in here is a poll loop against a deadline, so the clock is
    virtual for all of them: the deadline arithmetic runs exactly as it does in
    a real run, and a test that drives one to its timeout costs nothing.
    """

    def setUp(self) -> None:
        self.clock = self.no_waiting()

    def sql(self, **kwargs: object) -> hf.ScriptedSql:
        connector = hf.ScriptedSql(**kwargs)  # type: ignore[arg-type]
        self.install_sql(connector, tr.set_connector)
        return connector


def mgmt_replies(leader: str | None, members: dict[str, object]) -> object:
    """Answer both endpoints the readiness gate reads, for every node."""

    def reply(_node: str, path: str) -> object:
        if path == "/cluster/leader":
            return {"leader_id": int(leader.removeprefix("node"))} if leader else None
        return members

    return reply


class SettledClusterTest(SeamTest):
    """A leader is not enough to damage a node's Raft store against.

    `await_leader` returns the moment the bootstrap node calls itself leader,
    which it does at a voter set of `{1}`. Tearing a node the cluster does not
    list measures nothing: `run_join_flow` wipes the local store and rejoins
    when the peer does not list this node, so pgbattery deletes the damage
    rather than opening it.
    """

    def settle(self, **world: object) -> str:
        self.install(cluster_runner(**world))  # type: ignore[arg-type]
        return tr.await_settled_cluster(0.05)

    def test_a_full_voter_set_settles(self) -> None:
        self.assertEqual(self.settle(), "node1")

    def test_a_bootstrap_only_voter_set_does_not_settle(self) -> None:
        """The exact CI state: node1 leading at `configs: [{1}]` with node2
        still being added as a learner."""
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            self.settle(voters=("node1",))
        self.assertIn("full voter set", str(caught.exception))

    def test_a_learner_does_not_count_as_a_voter(self) -> None:
        with self.assertRaises(fp.FaultPreconditionError):
            self.settle(voters=("node1", "node3"))

    def test_a_node_that_cannot_answer_does_not_settle(self) -> None:
        """A view collected only from reachable nodes would let a node that
        cannot serve its own management API count as a settled voter."""
        with self.assertRaises(fp.FaultPreconditionError):
            self.settle(silent=topology.NODES[-1])

    def test_the_precondition_names_what_stalled_the_missing_node(self) -> None:
        """A bare convergence timeout reads as a slow cluster. The two CI
        occurrences were a node shut down over a catalog it could not open, and
        one that met a populated data directory — both said so in their logs."""
        silent = topology.NODES[-1]
        corrupt = (
            'ERROR: index "pg_namespace_nspname_index" contains unexpected zero page at block 0'
        )
        with self.assertRaises(fp.FaultPreconditionError) as raised:
            self.settle(
                silent=silent,
                container="restarting",
                restarts=7,
                node_log=f"noise\n{corrupt}\nmore noise",
            )
        message = str(raised.exception)
        self.assertIn(silent, message)
        self.assertIn("unexpected zero page", message)
        self.assertIn("restarts=7", message)

    def test_a_stall_with_no_known_signature_says_so(self) -> None:
        """Silence would read as "nothing wrong with that node"."""
        self.assertIsNone(fp.stall_reason("just some ordinary startup chatter"))

    def test_the_most_recent_stall_line_wins(self) -> None:
        """A restart-looping node repeats its complaint; the last one is the
        state it is in now."""
        log = "FATAL: first thing that went wrong\nFATAL: what it is doing now"
        self.assertEqual(fp.stall_reason(log), "FATAL: what it is doing now")


class ProveOracleTest(SeamTest):
    """The inversion must not damage a node the cluster has not admitted."""

    def test_an_unsettled_cluster_is_not_mangled(self) -> None:
        runner = self.install(cluster_runner(voters=("node1",)))
        with self.assertRaises(fp.FaultPreconditionError):
            tr.prove_oracle()
        self.assertFalse(
            runner.matching(tr.MANGLE_MARKER),
            "the store of a node the cluster has not admitted was overwritten",
        )


class MangleScriptTest(unittest.TestCase):
    """Run the real shell. `dd of=` creates what it cannot find, so a store
    that does not exist yet would be invented, marked, and read straight back
    as damage."""

    def run_script(self, path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["sh", "-c", tr.mangle_script(str(path))],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_an_absent_store_is_refused_and_not_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "raft.db"
            result = self.run_script(missing)
            self.assertNotEqual(result.returncode, 0, "an absent store must not report damage")
            self.assertFalse(missing.exists(), "the script invented the store it claimed to tear")

    def test_an_empty_store_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "raft.db"
            empty.touch()
            self.assertNotEqual(self.run_script(empty).returncode, 0)

    def test_the_script_survives_the_callers_quoting(self) -> None:
        """`mangle_raft_db` wraps this in a single-quoted `-c` argument, so a
        single quote in the script would end that argument early and hand the
        container a truncated command that still exits 0."""
        self.assertNotIn("'", tr.mangle_script(tr.RAFT_DB))

    def test_a_real_store_is_overwritten_and_reads_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "raft.db"
            store.write_bytes(b"\0" * (tr.MANGLE_BYTES * 2))
            result = self.run_script(store)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn(result.stdout.strip().splitlines()[-1], ("", "0"))
            self.assertEqual(
                store.stat().st_size,
                tr.MANGLE_BYTES * 2,
                "conv=notrunc keeps the tail, so only the head is destroyed",
            )
            self.assertIn(tr.MANGLE_MARKER.encode(), store.read_bytes()[: tr.MANGLE_BYTES])


class TearReadingTest(unittest.TestCase):
    """One attempt must not report a log it could not read as a log that was empty."""

    def read(self, result: fp.CommandResult) -> tr.TearReading:
        return tr.read_tear(result)

    def test_unreadable_log_is_not_an_absent_tear(self) -> None:
        reading = self.read(fp.CommandResult(rc=1, stdout="", stderr="is not running"))
        self.assertTrue(
            reading.unreadable,
            "a log the harness could not read must be recorded as unread; reporting "
            "it as 'no tear' turns a fault that fired into one that never did",
        )
        self.assertIn("is not running", reading.unreadable)

    def test_readable_log_with_no_record_is_an_absent_tear(self) -> None:
        reading = self.read(fp.CommandResult(rc=0, stdout="nothing of interest\n", stderr=""))
        self.assertEqual(reading.unreadable, "")
        self.assertIsNone(reading.record)

    def test_readable_log_reports_the_last_tear(self) -> None:
        line = f"Write to path {tr.RAFT_DB}: will persist 1024 bytes from offset 8192"
        reading = self.read(fp.CommandResult(rc=0, stdout=f"{line}\n", stderr=""))
        self.assertEqual(reading.record, (1024, 8192))
        self.assertEqual(reading.unreadable, "")


class WriteBatchTest(SeamTest):
    """Writes driven while a tear is armed race the fault by design."""

    def test_a_connection_lost_under_the_fault_is_not_an_error(self) -> None:
        """Tearing the leader's store kills it mid-batch, which is the fault
        working. Before this the whole run died on the dropped connection."""
        self.sql(connect_raises=psycopg.OperationalError("server closed the connection"))
        self.assertEqual(tr.write_batch("node1", range(5), under_fault=True), [])

    def test_a_connection_lost_with_no_fault_armed_still_raises(self) -> None:
        """The batch before the first tear establishes the baseline. Swallowing
        its failure would leave the run measuring an empty acked set."""
        self.sql(connect_raises=psycopg.OperationalError("server closed the connection"))
        with self.assertRaises(psycopg.Error):
            tr.write_batch("node1", range(5))

    def test_a_statement_that_raised_was_never_acked(self) -> None:
        self.sql(raises={"INSERT": hf.duplicate_key()}, heals_after=2)
        self.assertEqual(tr.write_batch("node1", range(5)), [2, 3, 4])


class DamageEstablishedTest(unittest.TestCase):
    """What entitles a run to assert anything about torn-write handling."""

    def test_a_sized_tear_establishes_damage(self) -> None:
        self.assertTrue(outcome(tears=1, victim_healthy=True).damage_established)

    def test_a_refusal_on_a_corrupt_store_establishes_damage(self) -> None:
        self.assertTrue(
            outcome(tears=0, victim_refused=True, refusal=tr.HANDLED_REFUSAL).damage_established,
            "a node that will not open the store it was just serving is damage no "
            "byte threshold would have judged more strictly",
        )

    def test_a_refusal_nobody_can_attribute_establishes_nothing(self) -> None:
        self.assertFalse(
            outcome(tears=0, victim_refused=True, refusal=tr.UNKNOWN_REFUSAL).damage_established,
            "a node down for reasons the harness cannot name is not evidence the "
            "tear reached the store",
        )

    def test_a_healthy_node_with_no_sized_tear_establishes_nothing(self) -> None:
        self.assertFalse(outcome(tears=0, victim_healthy=True).damage_established)


class UnobservedFaultMessageTest(unittest.TestCase):
    """The message must say which of the two happened, because they differ."""

    def test_unreadable_attempts_are_named_rather_than_denied(self) -> None:
        message = tr.unobserved_fault_message(
            outcome(attempts=3, unreadable=["node2: is not running"]), min_torn_bytes=512
        )
        self.assertIn("could not be read", message)
        self.assertNotIn("the fault never fired", message)

    def test_a_readable_empty_log_says_the_fault_never_fired(self) -> None:
        message = tr.unobserved_fault_message(outcome(attempts=3), min_torn_bytes=512)
        self.assertIn("never fired", message)


class WritableLeaderTest(SeamTest):
    """A settled voter set is not a serving data plane.

    A node that has just rejoined leaves the leader fenced until the sync
    durability it promises is being delivered again. Waiting only for
    membership let a run start its baseline into that window, have all 200
    writes refused, and report "no working cluster to damage" — accurate, but
    naming the symptom two steps after the cause.
    """

    def test_a_leader_that_takes_a_write_is_returned_at_once(self) -> None:
        self.install(cluster_runner())
        sql = self.sql()
        self.assertEqual(tr.await_writable_leader(30.0), "node1")
        self.assertTrue(sql.issued("DELETE"), f"no write was attempted: {sql.statements}")

    def test_a_fenced_leader_is_waited_out_rather_than_used(self) -> None:
        self.install(cluster_runner())
        self.sql(raises={"DELETE": hf.read_only()}, heals_after=1)
        self.assertEqual(tr.await_writable_leader(30.0), "node1")

    def test_a_leader_that_never_takes_a_write_refuses_the_run(self) -> None:
        self.install(cluster_runner())
        self.sql(raises={"DELETE": hf.read_only()})
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            tr.await_writable_leader(0.05)
        self.assertIn("read-only transaction", str(caught.exception))

    def test_the_probe_issues_a_write_and_keeps_none_of_it(self) -> None:
        # Two traps this gate fell into. CREATE TABLE IF NOT EXISTS against an
        # existing table is not a write, so it succeeds on a fenced primary and
        # the gate passes into a cluster that refuses every row a moment later.
        # And `connect` is autocommit, so a probe that inserts keeps the row
        # and the second call fails on the primary key it wrote itself.
        sql = self.sql()
        tr.accepts_a_write("node1")
        tr.accepts_a_write("node1")
        self.assertTrue(sql.issued("DELETE"), f"no write attempted: {sql.statements}")
        self.assertFalse(
            sql.issued("INSERT"),
            f"an autocommit probe that inserts cannot repeat: {sql.statements}",
        )

    def test_a_cluster_with_no_leader_never_reaches_the_write_probe(self) -> None:
        # The settle refuses first, and says which nodes were missing. Reaching
        # the probe at all would mean tearing a node the cluster had not
        # admitted, which measures nothing.
        self.install(cluster_runner(leader=None))
        sql = self.sql()
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            tr.await_writable_leader(0.05)
        self.assertIn("not a full voter set", str(caught.exception))
        self.assertEqual(sql.statements, [])


class VictimRecreateTest(SeamTest):
    """Destroying the container discards both LazyFS caches, not just the one
    the run is aiming at. PGDATA is checkpointed first so exactly one store is
    damaged; the Raft mount is left dirty because that is the fault."""

    def test_pgdata_is_checkpointed_and_the_raft_store_is_not(self) -> None:
        runner = self.install(cluster_runner(container="running"))
        tr.recreate_victim("node2")
        self.assertTrue(runner.matching(fp.LAZYFS_DATA.fifo), "PGDATA was never checkpointed")
        self.assertFalse(
            runner.matching(fp.LAZYFS_RAFT.fifo),
            "the Raft mount was checkpointed; its un-fsynced state is the fault under test",
        )
        self.assertTrue(runner.matching("--force-recreate"))

    def test_a_cache_that_will_not_flush_stops_the_run(self) -> None:
        # No `checkpoint is done` in the log, so the flush cannot be confirmed.
        runner = self.install(cluster_runner(container="running", checkpoints=False))
        with self.assertRaises(fp.FaultEffectNotObserved):
            tr.recreate_victim("node2")
        self.assertFalse(
            runner.matching("--force-recreate"),
            "the container was destroyed before its cache was known to be safe",
        )

    def test_a_stopped_container_is_recreated_without_waiting_on_a_flush(self) -> None:
        # Reached from the failure paths, where a flush that timed out would
        # replace the failure it was called to protect against.
        # "" is the container compose no longer knows: `read_container_runstate`
        # cannot resolve an id and raises, which is the state a run that died
        # mid-fault leaves behind and the one a `finally` must survive.
        for state in ("exited", ""):
            runner = self.install(cluster_runner(container=state))
            tr.recreate_victim("node2")
            self.assertFalse(runner.matching(fp.LAZYFS_DATA.fifo))
            self.assertTrue(runner.matching("--force-recreate"))


class BaselineIsolationTest(SeamTest):
    """The CI job runs the header tear and then the page tear on one cluster.

    Both write the same key range, so without a reset every insert in the
    second run is a duplicate-key violation. `write_batch` counts those as
    unacked, correctly — which is how a healthy cluster came to be reported as
    "no working cluster to damage".
    """

    def test_the_baseline_starts_from_an_empty_table(self) -> None:
        sql = self.sql()
        tr.establish_baseline("node1")
        self.assertTrue(
            sql.issued("TRUNCATE"), f"a second run inherits the first run's keys: {sql.statements}"
        )

    def test_a_baseline_the_cluster_refuses_stops_the_run(self) -> None:
        # Exactly the shape the CI job hit: a healthy cluster, and every insert
        # colliding with the previous run's keys.
        self.sql(raises={"INSERT": hf.duplicate_key()})
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            tr.establish_baseline("node1")
        self.assertIn("baseline writes were acked", str(caught.exception))

    def test_a_mostly_refused_baseline_stops_it_too(self) -> None:
        self.sql(raises={"INSERT": hf.duplicate_key()}, heals_after=tr.BASELINE_WRITES - 10)
        with self.assertRaises(fp.FaultPreconditionError):
            tr.establish_baseline("node1")

    def test_a_baseline_that_lands_is_what_survival_is_measured_against(self) -> None:
        self.sql()
        self.assertEqual(len(tr.establish_baseline("node1")), tr.BASELINE_WRITES)


class MakeLeaderTest(SeamTest):
    """Arming a config-baked fault restarts the victim, which moves leadership
    off it. A leader-targeted run has to put it back or it tears a follower."""

    def test_leadership_already_in_place_asks_for_nothing(self) -> None:
        runner = self.install(cluster_runner(leader="node2"))
        tr.make_leader("node2", 5.0)
        self.assertFalse(
            runner.matching("transfer-leadership"),
            "a transfer nobody needs is a leadership change nobody asked for",
        )

    def test_leadership_that_moves_is_accepted(self) -> None:
        runner = self.install(TransferRunner("node1", "node2"))
        tr.make_leader("node2", 60.0)
        self.assertTrue(runner.matching("transfer-leadership/2"))

    def test_leadership_that_will_not_move_refuses_the_run(self) -> None:
        self.install(cluster_runner(leader="node1"))
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            tr.make_leader("node2", 0.05)
        self.assertIn("would have damaged a follower", str(caught.exception))


class MgmtTokenTest(SeamTest):
    """CI passes the token in the environment; compose reads it from `.env`."""

    def test_the_environment_wins(self) -> None:
        with mock.patch.dict(os.environ, {"PGBATTERY_MANAGEMENT_API_TOKEN": "from-env"}):
            self.assertEqual(tr.mgmt_token(), "from-env")

    def test_dot_env_is_the_fallback(self) -> None:
        self.install(
            hf.ScriptedRunner([("PGBATTERY_MANAGEMENT_API_TOKEN", hf.ok("from-dot-env\n"))])
        )
        with mock.patch.dict(os.environ, {"PGBATTERY_MANAGEMENT_API_TOKEN": ""}):
            self.assertEqual(tr.mgmt_token(), "from-dot-env")


class PageTearDisarmTest(SeamTest):
    """A config-baked torn-op outlives the run that armed it.

    Unlike the FIFO form, which is consumed when it fires, an `[[injection]]`
    block stays in the container's config and rearms on every restart. Only
    recreating the container drops it, so the page tear has to do that on the
    paths where it fails too — otherwise it hands the next suite a node that
    tears a write nobody asked for.
    """

    def test_a_landed_tear_recreates_the_victim(self) -> None:
        runner = self.install(cluster_runner(torn_record=(2048, 49152)))
        self.sql()
        outcome = tr.tear_a_page(target="follower", occurrence=tr.PAGE_TEAR_OCCURRENCE)
        self.assertEqual(outcome.torn_offsets, [49152])
        self.assertTrue(runner.matching("--force-recreate"))

    def test_a_fault_that_never_fired_still_recreates_the_victim(self) -> None:
        # No `will persist` line, so `await_torn_record` times out and the run
        # unwinds through the `finally` that disarms the victim.
        runner = self.install(cluster_runner(torn_record=None))
        self.sql()
        with self.assertRaises(fp.FaultEffectNotObserved):
            tr.tear_a_page(target="follower", occurrence=tr.PAGE_TEAR_OCCURRENCE)
        self.assertTrue(
            runner.matching("--force-recreate"),
            "a run that failed to tear must still leave the victim disarmed",
        )

    def test_a_tear_that_only_reaches_the_header_is_not_claimed_as_a_page(self) -> None:
        self.install(cluster_runner(torn_record=(160, 0)))
        self.sql()
        with self.assertRaises(fp.FaultEffectNotObserved) as caught:
            tr.tear_a_page(target="follower", occurrence=tr.PAGE_TEAR_OCCURRENCE)
        self.assertIn("commit header at offset 0", str(caught.exception))


class RestartClusterTests(unittest.TestCase):
    """The rebuild between the inversion and the real run."""

    def install(self, runner: hf.ScriptedRunner) -> hf.ScriptedRunner:
        previous = fp.set_command_runner(runner)
        self.addCleanup(fp.set_command_runner, previous)
        return runner

    def test_a_teardown_that_did_not_remove_the_volumes_is_not_built_on(self) -> None:
        """A node whose volume survived comes back holding a data directory
        from a cluster the others no longer are; `join` refuses to discard data
        of unproven lineage and the node restarts into that refusal for the
        rest of the run. The symptom is a voter set that never fills, which
        says nothing about the teardown that caused it."""
        runner = self.install(
            hf.ScriptedRunner(
                [
                    (
                        "docker compose down -v",
                        hf.fail("Error response from daemon: volume is in use"),
                    )
                ]
            )
        )
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            tr.reset_cluster()
        self.assertIn("tear the cluster down", str(caught.exception))
        self.assertEqual(
            runner.matching("docker compose up -d"),
            [],
            "brought a cluster up on volumes it failed to remove",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
