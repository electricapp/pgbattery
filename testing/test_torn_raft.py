#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the Raft torn-write suite's verdict logic.

No docker. These cover what the suite concludes from an attempt it could not
fully observe, which is the part that decides whether a contract may be claimed.

Run with:
    uv run --project testing python testing/test_torn_raft.py
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import psycopg

import fault_primitives as fp
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


def mgmt_replies(leader: str | None, members: dict[str, object]) -> object:
    """Answer both endpoints the readiness gate reads, for every node."""

    def reply(_node: str, path: str) -> object:
        if path == "/cluster/leader":
            return {"leader_id": int(leader.removeprefix("node"))} if leader else None
        return members

    return reply


class SettledClusterTest(unittest.TestCase):
    """A leader is not enough to damage a node's Raft store against.

    `await_leader` returns the moment the bootstrap node calls itself leader,
    which it does at a voter set of `{1}`. Tearing a node the cluster does not
    list measures nothing: `run_join_flow` wipes the local store and rejoins
    when the peer does not list this node, so pgbattery deletes the damage
    rather than opening it.
    """

    def settle(self, leader: str | None, members: dict[str, object]) -> str:
        with (
            mock.patch.object(tr, "mgmt", side_effect=mgmt_replies(leader, members)),
            mock.patch("time.sleep"),
        ):
            return tr.await_settled_cluster(0.05)

    def test_a_full_voter_set_settles(self) -> None:
        self.assertEqual(self.settle("node1", membership(*topology.NODES)), "node1")

    def test_a_bootstrap_only_voter_set_does_not_settle(self) -> None:
        """The exact CI state: node1 leading at `configs: [{1}]` with node2
        still being added as a learner."""
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            self.settle("node1", membership("node1", learners=("node2",)))
        self.assertIn("full voter set", str(caught.exception))

    def test_a_learner_does_not_count_as_a_voter(self) -> None:
        with self.assertRaises(fp.FaultPreconditionError):
            self.settle("node1", membership("node1", "node3", learners=("node2",)))

    def test_a_node_that_cannot_answer_does_not_settle(self) -> None:
        """A view collected only from reachable nodes would let a node that
        cannot serve its own management API count as a settled voter."""
        full = membership(*topology.NODES)
        silent = topology.NODES[-1]

        def reply(node: str, path: str) -> object:
            if node == silent:
                return None
            return mgmt_replies("node1", full)(node, path)  # type: ignore[operator]

        with (
            mock.patch.object(tr, "mgmt", side_effect=reply),
            mock.patch("time.sleep"),
            self.assertRaises(fp.FaultPreconditionError),
        ):
            tr.await_settled_cluster(0.05)

    def test_the_precondition_names_what_stalled_the_missing_node(self) -> None:
        """A bare convergence timeout reads as a slow cluster. The two CI
        occurrences were a node shut down over a catalog it could not open, and
        one that met a populated data directory — both said so in their logs."""
        full = membership(*topology.NODES)
        silent = topology.NODES[-1]
        corrupt = (
            'ERROR: index "pg_namespace_nspname_index" contains unexpected zero page at block 0'
        )

        def reply(node: str, path: str) -> object:
            if node == silent:
                return None
            return mgmt_replies("node1", full)(node, path)  # type: ignore[operator]

        with (
            mock.patch.object(tr, "mgmt", side_effect=reply),
            mock.patch.object(
                fp,
                "read_container_runstate",
                return_value=fp.ContainerRunState(
                    status="restarting", started_at="t", restart_count=7
                ),
            ),
            mock.patch.object(
                fp, "run", return_value=fp.CommandResult(0, f"noise\n{corrupt}\nmore noise", "")
            ),
            mock.patch("time.sleep"),
            self.assertRaises(fp.FaultPreconditionError) as raised,
        ):
            tr.await_settled_cluster(0.05)
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


class ProveOracleTest(unittest.TestCase):
    """The inversion must not damage a node the cluster has not admitted."""

    def test_an_unsettled_cluster_is_not_mangled(self) -> None:
        with (
            mock.patch.object(tr, "mgmt", side_effect=mgmt_replies("node1", membership("node1"))),
            mock.patch.object(tr, "mangle_raft_db") as mangle,
            mock.patch.object(tr, "CONVERGE_TIMEOUT_S", 0.05),
            mock.patch("time.sleep"),
            self.assertRaises(fp.FaultPreconditionError),
        ):
            tr.prove_oracle()
        mangle.assert_not_called()


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
        with (
            mock.patch.object(fp, "arm_torn_write"),
            mock.patch.object(tr, "await_leader", return_value="node1"),
            mock.patch.object(tr, "write_batch", return_value=[]),
            mock.patch("time.sleep"),
            mock.patch.object(fp, "exec_when_deliverable", return_value=result),
        ):
            return tr.tear_once("node2", 0, tr.Outcome())

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


class WriteBatchTest(unittest.TestCase):
    """Writes driven while a tear is armed race the fault by design."""

    def test_a_connection_lost_under_the_fault_is_not_an_error(self) -> None:
        """Tearing the leader's store kills it mid-batch, which is the fault
        working. Before this the whole run died on the dropped connection."""
        with mock.patch.object(
            tr, "connect", side_effect=psycopg.OperationalError("server closed the connection")
        ):
            self.assertEqual(tr.write_batch("node1", range(5), under_fault=True), [])

    def test_a_connection_lost_with_no_fault_armed_still_raises(self) -> None:
        """The batch before the first tear establishes the baseline. Swallowing
        its failure would leave the run measuring an empty acked set."""
        with (
            mock.patch.object(
                tr, "connect", side_effect=psycopg.OperationalError("server closed the connection")
            ),
            self.assertRaises(psycopg.Error),
        ):
            tr.write_batch("node1", range(5))


class BaselineAckedTest(unittest.TestCase):
    """`lost` is `acked - surviving`, so an empty acked set makes "no acked write
    lost" hold for want of anything to lose."""

    def run_tears_with_baseline(self, acked: list[int]) -> None:
        with (
            mock.patch.object(tr, "await_settled_cluster", return_value="node1"),
            mock.patch.object(tr, "ensure_table"),
            mock.patch.object(tr, "write_batch", return_value=acked),
        ):
            tr.run_tears(tears=1, target="follower", min_torn_bytes=512, max_attempts=1)

    def test_a_cluster_that_took_no_writes_fails_before_any_tear(self) -> None:
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            self.run_tears_with_baseline([])
        self.assertIn("baseline writes were acked", str(caught.exception))

    def test_a_mostly_failed_baseline_fails_too(self) -> None:
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            self.run_tears_with_baseline(list(range(10)))
        self.assertIn("baseline writes were acked", str(caught.exception))


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
