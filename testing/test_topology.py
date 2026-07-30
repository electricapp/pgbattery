#!/usr/bin/env -S uv run --project testing python
"""Proof that the topology derivation fails loudly rather than emptily.

Every harness now addresses docker objects through `topology.py`, which reads
the compose file instead of restating it. That removes four independent copies
of the same facts, and replaces them with one dependency on a parse — so the
parse is now the thing that can go wrong quietly.

Quietly is the danger. A derivation that yields an empty node list turns every
`for node in NODES` into a no-op: no fault is injected, no assertion runs, and
the suite reports PASS. That is the exact shape this repo has been bitten by
five times, moved one level up. So every failure to understand the compose file
raises, and these cases are the proof.

The second half ties the derivation to the real files. Synthetic YAML alone
would keep passing after `docker-compose.yml` changed shape.

Run with:
    uv run --project testing python testing/test_topology.py
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, Final

import topology
from topology import TopologyError

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

MINIMAL_COMPOSE: Final[str] = """
services:
  node1:
    command: ["sh", "-c", "exec pgbattery run --bootstrap"]
    volumes:
      - ./config/node1.toml:/app/pgbattery.toml:ro
    ports:
      - "5432:5432"
      - "9091:9090"
      - "9081:9091"
    networks:
      raft_net:
        ipv4_address: 172.28.0.11
networks:
  raft_net:
"""


def write_compose(body: str) -> Path:
    """Write a compose document to a temp file and return its path."""
    path = Path(tempfile.mkdtemp(prefix="topology-")) / "docker-compose.yml"
    path.write_text(body, encoding="utf-8")
    return path


class DerivationSucceedsOnAWellFormedFile(unittest.TestCase):
    """Without this, every rejection case below would also pass against a
    parser that rejected everything."""

    def test_a_minimal_node_is_read(self) -> None:
        loaded = topology.load(write_compose(MINIMAL_COMPOSE))
        (node,) = loaded.nodes
        self.assertEqual(node.service, "node1")
        self.assertEqual(node.node_id, 1)
        self.assertEqual(node.ip, "172.28.0.11")
        self.assertEqual(node.gateway_port, 5432)
        self.assertEqual(node.metrics_port, 9091)
        self.assertEqual(node.mgmt_port, 9081)
        self.assertTrue(node.is_voter)

    def test_long_form_port_syntax_is_read(self) -> None:
        """Compose accepts both spellings, so a file that switches must not
        silently produce a node with no ports."""
        long_form = MINIMAL_COMPOSE.replace(
            '      - "5432:5432"\n      - "9091:9090"\n      - "9081:9091"\n',
            "      - {target: 5432, published: 5432}\n"
            "      - {target: 9090, published: 9091}\n"
            "      - {target: 9091, published: 9081}\n",
        )
        (node,) = topology.load(write_compose(long_form)).nodes
        self.assertEqual((node.gateway_port, node.metrics_port, node.mgmt_port), (5432, 9091, 9081))


class MalformedFilesRaise(unittest.TestCase):
    def test_a_file_with_no_nodes_is_an_error(self) -> None:
        """The one that matters most. An empty node list makes every loop over
        it a no-op, and a suite of no-ops reports PASS."""
        empty = "services:\n  builder:\n    image: busybox\nnetworks:\n  raft_net:\n"
        with self.assertRaises(TopologyError) as raised:
            topology.load(write_compose(empty))
        self.assertIn("no pgbattery nodes", str(raised.exception))

    def test_a_missing_file_is_an_error(self) -> None:
        with self.assertRaises(TopologyError):
            topology.load(Path("/nonexistent/docker-compose.yml"))

    def test_a_node_missing_one_port_mapping_is_an_error(self) -> None:
        """Publishing two of the three is a broken deployment, not a sidecar:
        callers would look up the third and address a port that is not there."""
        partial = MINIMAL_COMPOSE.replace('      - "9081:9091"\n', "")
        with self.assertRaises(TopologyError) as raised:
            topology.load(write_compose(partial))
        self.assertIn("publishes some pgbattery ports", str(raised.exception))

    def test_a_service_publishing_none_of_them_is_skipped_not_rejected(self) -> None:
        """A workload container or sidecar is part of the deployment without
        being a node."""
        with_sidecar = MINIMAL_COMPOSE.replace(
            "networks:\n  raft_net:\n",
            '  sidecar:\n    image: busybox\n    ports:\n      - "8080:8080"\n'
            "networks:\n  raft_net:\n",
        )
        services = [n.service for n in topology.load(write_compose(with_sidecar)).nodes]
        self.assertEqual(services, ["node1"])

    def test_a_node_without_a_static_address_is_an_error(self) -> None:
        """Peer-level faults address nodes by IP; a DHCP address would move
        between runs and the DROP rule would land on nothing."""
        dhcp = MINIMAL_COMPOSE.replace(
            "      raft_net:\n        ipv4_address: 172.28.0.11\n", "      - raft_net\n"
        )
        with self.assertRaises(TopologyError):
            topology.load(write_compose(dhcp))

    def test_a_node_without_a_config_mount_is_an_error(self) -> None:
        """Identity comes from the config the container reads, so a service
        without one cannot be given a node id at all."""
        unmounted = MINIMAL_COMPOSE.replace(
            "    volumes:\n      - ./config/node1.toml:/app/pgbattery.toml:ro\n", ""
        )
        with self.assertRaises(TopologyError) as raised:
            topology.load(write_compose(unmounted))
        self.assertIn("declared identity", str(raised.exception))


class VoterSetComesFromTheStartCommand(unittest.TestCase):
    """Counting a learner as a voter made the first 5-node run's quorum
    assertions vacuous: it killed two learners and called it two voters."""

    def test_bootstrap_and_voter_joins_are_voters(self) -> None:
        (node,) = topology.load(write_compose(MINIMAL_COMPOSE)).nodes
        self.assertTrue(node.is_voter)
        as_voter = MINIMAL_COMPOSE.replace(
            "exec pgbattery run --bootstrap", "exec pgbattery join --peer x --node-id 1 --voter"
        )
        (joined,) = topology.load(write_compose(as_voter)).nodes
        self.assertTrue(joined.is_voter)

    def test_a_plain_join_is_a_learner(self) -> None:
        as_learner = MINIMAL_COMPOSE.replace(
            "exec pgbattery run --bootstrap", "exec pgbattery join --peer x --node-id 1"
        )
        loaded = topology.load(write_compose(as_learner))
        self.assertFalse(loaded.nodes[0].is_voter)
        self.assertEqual(loaded.voters, ())


class TheRealComposeFilesAreUnderstood(unittest.TestCase):
    """Ties the synthetic cases above to the files the harnesses actually run
    against. Without this, `docker-compose.yml` could change shape and only the
    live suite would find out."""

    def test_three_node_compose(self) -> None:
        loaded = topology.load(REPO_ROOT / "docker-compose.yml")
        self.assertEqual(loaded.voter_services, ("node1", "node2", "node3"))
        # The witness is in the deployment but not in the voter set.
        self.assertIn("witness", [n.service for n in loaded.nodes])
        self.assertEqual(loaded.by_service("witness").node_id, 4)
        self.assertFalse(loaded.by_service("witness").is_voter)

    def test_five_node_compose(self) -> None:
        loaded = topology.load(REPO_ROOT / "docker-compose.5node.yml")
        self.assertEqual(len(loaded.voters), 5)
        self.assertEqual([n.node_id for n in loaded.voters], [1, 2, 3, 4, 5])

    def test_the_two_clusters_do_not_share_addresses(self) -> None:
        """Both compose files can be up at once, so an overlap would make a
        peer-level fault in one land in the other."""
        three = {n.ip for n in topology.load(REPO_ROOT / "docker-compose.yml").nodes}
        five = {n.ip for n in topology.load(REPO_ROOT / "docker-compose.5node.yml").nodes}
        self.assertEqual(three & five, set(), "the two clusters share an address")

    def test_published_host_ports_are_unique_within_a_cluster(self) -> None:
        for name in ("docker-compose.yml", "docker-compose.5node.yml"):
            with self.subTest(compose=name):
                loaded = topology.load(REPO_ROOT / name)
                published = [
                    port
                    for node in loaded.nodes
                    for port in (node.gateway_port, node.metrics_port, node.mgmt_port)
                ]
                self.assertEqual(
                    len(published), len(set(published)), f"{name} publishes a port twice"
                )


class HarnessesAgreeWithTheDerivation(unittest.TestCase):
    """The point of the module: one answer, not five. If any of these
    reintroduced its own copy, this fails."""

    def test_every_harness_sees_the_same_three_node_cluster(self) -> None:
        import correctness_lite
        import dual_writability_prober as prober
        import fault_primitives as fp
        from linreg import cluster

        expected = list(topology.NODES)
        self.assertEqual(list(fp.NODES), expected)
        self.assertEqual(list(cluster.NODES), expected)
        self.assertEqual(correctness_lite.NODES, expected)
        self.assertEqual([n.service for n in prober.NODES], expected)

    def test_the_prober_covers_every_voter_in_both_topologies(self) -> None:
        """L1 is cluster-wide: a node the prober never asks cannot be observed
        accepting a write, and the run would report at most one acceptance
        having never looked."""
        import dual_writability_prober as prober

        for name, targets in (
            ("docker-compose.yml", prober.NODES),
            ("docker-compose.5node.yml", prober.FIVE_NODES),
        ):
            with self.subTest(compose=name):
                voters = topology.load(REPO_ROOT / name).voters
                self.assertEqual(
                    [t.node_id for t in targets],
                    [n.node_id for n in voters],
                    f"the prober does not cover every voter in {name}",
                )

    def test_the_five_node_suite_matches_its_compose_file(self) -> None:
        import five_node_suite

        five = topology.load(REPO_ROOT / five_node_suite.COMPOSE_FILE)
        self.assertEqual(list(five_node_suite.NODES), [n.node_id for n in five.voters])
        self.assertEqual(five_node_suite.NODE_IP, {n.node_id: n.ip for n in five.nodes})
        self.assertEqual(five_node_suite.MGMT_PORT, {n.node_id: n.mgmt_port for n in five.voters})


class MatrixClusterReconciliation(unittest.TestCase):
    """`ci_runner.py` builds its node map from `ci_matrix.yaml`, which is the
    one place the topology is still written down twice. The lint that
    reconciles the two has to be seen catching each way they can diverge — a
    runner polling a port nothing listens on reads no metrics and concludes the
    node is down."""

    @staticmethod
    def _real() -> tuple[dict[str, Any], topology.Topology]:
        import json

        data = json.loads((REPO_ROOT / "testing" / "ci_matrix.yaml").read_text(encoding="utf-8"))
        return data["cluster"], topology.load(REPO_ROOT / data["compose_file"])

    def test_the_real_matrix_agrees_with_the_real_compose_file(self) -> None:
        import lint_matrix

        cluster, derived = self._real()
        self.assertEqual(lint_matrix.cluster_topology_mismatches(cluster, derived), [])

    def test_a_wrong_management_port_is_caught(self) -> None:
        import copy

        import lint_matrix

        cluster, derived = self._real()
        broken = copy.deepcopy(cluster)
        broken["nodes"][0]["mgmt_url"] = "http://localhost:9999"
        found = lint_matrix.cluster_topology_mismatches(broken, derived)
        self.assertTrue(any("mgmt_url" in problem for problem in found), found)

    def test_a_wrong_service_name_is_caught(self) -> None:
        import copy

        import lint_matrix

        cluster, derived = self._real()
        broken = copy.deepcopy(cluster)
        broken["nodes"][1]["name"] = "node-two"
        found = lint_matrix.cluster_topology_mismatches(broken, derived)
        self.assertTrue(any("name" in problem for problem in found), found)

    def test_a_missing_node_is_caught(self) -> None:
        import copy

        import lint_matrix

        cluster, derived = self._real()
        broken = copy.deepcopy(cluster)
        broken["nodes"].pop()
        found = lint_matrix.cluster_topology_mismatches(broken, derived)
        self.assertTrue(found, "a matrix short one node was accepted")


if __name__ == "__main__":
    unittest.main()
