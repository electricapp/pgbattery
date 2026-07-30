"""Shell execution and leader discovery against the running compose cluster."""

from __future__ import annotations

import contextlib
import json
import subprocess
import time
from typing import Final

import topology

# Derived from the compose file, not restated here: the ports and service names
# used to be declared independently in this module, `fault_primitives.py`,
# `correctness_lite.py`, and `dual_writability_prober.py`, so a port change in
# compose left four harnesses quietly talking to nothing.
NODES: Final[list[str]] = list(topology.NODES)
GATEWAY_PORTS: Final[list[int]] = list(topology.GATEWAY_PORTS)
MGMT_PORTS: Final[list[int]] = [topology.MGMT_PORTS[node] for node in NODES]


def run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
    """Run a shell command, return (rc, stdout, stderr). -1 rc on timeout."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    return r.returncode, r.stdout, r.stderr


def find_leader() -> tuple[str | None, int | None]:
    """Return (node_name, gateway_port) for the current leader, or (None, None)."""
    for port in MGMT_PORTS:
        rc, out, _ = run_cmd(
            f"curl -sf --max-time 2 http://localhost:{port}/api/v1/cluster/leader",
            timeout=4,
        )
        if rc == 0:
            with contextlib.suppress(Exception):
                lid = json.loads(out).get("leader_id")
                if lid is not None and 1 <= lid <= len(NODES):
                    return NODES[lid - 1], GATEWAY_PORTS[lid - 1]
    return None, None


def wait_cluster_healthy(timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader, _ = find_leader()
        if leader is not None:
            return True
        time.sleep(2)
    return False
