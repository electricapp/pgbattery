#!/usr/bin/env -S uv run --project testing python
"""Track C blue/green cutover e2e (UPGRADE_CHECKLIST step 9).

``COMPOSE_FILE=docker-compose.bluegreen.yml`` — ports from topology.py only.

Phases:
  1. Wait blue + green Raft healthy; refuse unless blue@17 / green@18
  2. Seed schema + logical publication (blue) / subscription (green)
  3. Writes on blue's gateway, each acked key recorded for the U2 ledger
  4. Cutover: arm -> seal (server fences, then reads its own final LSN)
     -> drain observed against that LSN -> sequences advanced -> reverse
     slot -> transfer -> green accepts authority
  5. U2 (every pre-seal ack present on green), U3 (no sequence collision),
     U1 (six-port prober as the app role)
  6. Optional --chaos {arm-seal,seal-transfer,mid-drain,blue-quorum}
  7. Optional --rollback-drill: reverse protocol, green -> blue

Exit 0 on success; non-zero on contract failure or infra error.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg
import typer
from rich.console import Console

os.environ.setdefault("COMPOSE_FILE", "docker-compose.bluegreen.yml")

import topology
from topology import Node, Topology, load
from waiting import WaitTimeout, await_until

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
APP_ROLE: Final[str] = "bg_app"
# The U1 prober's table, created here as the superuser and granted to the app
# role. Letting the prober create it would need CREATE on schema public, and a
# role that can create a table it owns writes to it whatever the fence revoked.
PROBE_TABLE_DDL: Final[str] = f"""
CREATE TABLE IF NOT EXISTS pgb_dual_write_probe (
  node_id   integer PRIMARY KEY,
  round_seq bigint  NOT NULL,
  probe_ns  bigint  NOT NULL
);
GRANT SELECT, INSERT, UPDATE, DELETE ON pgb_dual_write_probe TO {APP_ROLE};
"""
# Bounds any single docker call; the image is built by the job beforehand.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0
SUB_OWNER: Final[str] = "postgres"
TOKEN_ENV: Final[str] = "PGBATTERY_MANAGEMENT_API_TOKEN"
BLUE_CLUSTER: Final[str] = "blue"
GREEN_CLUSTER: Final[str] = "green"
BLUE_MAJOR: Final[int] = 17
GREEN_MAJOR: Final[int] = 18
ARTIFACT_DIR: Final[Path] = PROJECT_ROOT / "testing/artifacts/bluegreen-u1"

console = Console()
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """A Track C contract did not hold. Never an infrastructure problem."""


def _token() -> str:
    t = os.environ.get(TOKEN_ENV, "").strip()
    if not t:
        raise SystemExit(f"{TOKEN_ENV} required")
    return t


def _headers() -> dict[str, str]:
    return {"x-pgbattery-token": _token()}


def _mgmt(node: Node) -> str:
    return f"http://127.0.0.1:{node.mgmt_port}"


def _pg_addr(node: Node) -> str:
    return f"{node.ip}:{topology.PG_INTERNAL_PORT}"


def _compose(
    *args: str, check: bool = False, timeout: float = COMPOSE_TIMEOUT_SEC
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=PROJECT_ROOT,
            check=check,
            text=True,
            capture_output=True,
            env={**os.environ},
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        # Naming the wedged command beats letting the step budget kill the
        # harness with nothing said about which docker call hung.
        raise SystemExit(f"docker compose {' '.join(args)} hung for {timeout:g}s") from exc


def _wait_leader(node: Node, timeout: float = 120.0) -> dict[str, Any]:
    last = ""

    def leader() -> dict[str, Any] | None:
        nonlocal last
        try:
            r = httpx.get(f"{_mgmt(node)}/api/v1/cluster/leader", timeout=2.0)
        except httpx.HTTPError as exc:
            last = str(exc)
            return None
        if r.status_code != 200:
            last = r.text
            return None
        body = r.json()
        if body.get("leader_id") is None:
            last = str(body)
            return None
        return {str(k): v for k, v in body.items()}

    try:
        return await_until(leader, budget=timeout, describe=f"a leader via {node.service}")
    except WaitTimeout as exc:
        raise TimeoutError(f"no leader via {node.service}: {last} ({exc})") from exc


def _psql(node: Node, sql: str, user: str = SUB_OWNER) -> str:
    """Run SQL against one node's own PostgreSQL, not through its gateway."""
    proc = _compose(
        "exec",
        "-T",
        node.service,
        "psql",
        "-U",
        user,
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-c",
        sql,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"psql on {node.service} failed: {proc.stderr or proc.stdout}")
    return proc.stdout.strip()


# The only errors the fence itself produces: the REVOKE-based client fence
# denies with 42501, a read-only-transaction fence with 25006. Anything else
# from the probe (missing role, missing table, refused connection) proves
# nothing about the fence and must never read as "fenced".
FENCE_DENIALS: Final[tuple[str, ...]] = (
    "permission denied",
    "read-only transaction",
)


def _classify_write_probe(returncode: int, stdout: str, stderr: str) -> bool:
    """True: the write went through. False: the fence explicitly denied it.

    Raises on any other failure — a broken probe is a broken probe, not a
    fence. Treating every non-zero exit as "fenced" is how the boot-fence
    assertion passed against a green that had no role to fence.
    """
    if returncode == 0:
        return True
    detail = f"{stderr}\n{stdout}".strip()
    if any(denial in detail for denial in FENCE_DENIALS):
        return False
    raise RuntimeError(f"fence probe broken, not fenced (rc={returncode}): {detail}")


def _psql_allowed(node: Node, sql: str, user: str) -> bool:
    """Whether `user` may run `sql` on `node`. Used to prove a fence holds.

    Raises when psql fails for any reason other than the fence's own denial.
    """
    proc = _compose(
        "exec",
        "-T",
        node.service,
        "psql",
        "-U",
        user,
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-c",
        sql,
    )
    return _classify_write_probe(proc.returncode, proc.stdout, proc.stderr)


def _blue_green(topo: Topology) -> tuple[list[Node], list[Node]]:
    blue = [n for n in topo.nodes if n.service.startswith("blue")]
    green = [n for n in topo.nodes if n.service.startswith("green")]
    if len(blue) != 3 or len(green) != 3:
        raise SystemExit(f"need 3+3 nodes, got {len(blue)}+{len(green)}")
    return blue, green


def _leader_node(nodes: list[Node]) -> Node:
    for n in nodes:
        try:
            body = _wait_leader(n, timeout=5.0)
            lid = int(body["leader_id"])
            for cand in nodes:
                if cand.node_id == lid:
                    return cand
        except TimeoutError:
            continue
    raise TimeoutError("no leader among cluster")


def _lsn_bytes(text: str) -> int:
    upper, lower = text.split("/")
    return (int(upper, 16) << 32) | int(lower, 16)


def _cutover_status(node: Node) -> dict[str, Any]:
    r = httpx.get(f"{_mgmt(node)}/api/v1/cutover/status", timeout=10.0)
    r.raise_for_status()
    return dict(r.json())


def _setup_schema(blue_leader: Node) -> None:
    _psql(
        blue_leader,
        f"""
        DO $$ BEGIN
          CREATE ROLE {APP_ROLE} LOGIN;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        GRANT CONNECT ON DATABASE postgres TO {APP_ROLE};
        CREATE TABLE IF NOT EXISTS bg_kv (
          k text PRIMARY KEY,
          v text NOT NULL,
          seq bigserial
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON bg_kv TO {APP_ROLE};
        GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE};
        {PROBE_TABLE_DDL}
        """,
    )


def _setup_green_schema(green_leader: Node) -> None:
    """Green's role and tables, before any fence probe targets them.

    Logical replication carries rows, not DDL, so green needs the schema
    either way. It must exist before the boot-fence probe: probing a green
    with no `bg_app` role fails on the missing role, which is not a fence.
    `seq` is a real identity column so pg_get_serial_sequence resolves it —
    a plain bigint would make the U3 setval a silent no-op. The whole batch
    is one implicit transaction, so the fence reconciler never observes the
    role without its grants.
    """
    _psql(
        green_leader,
        f"""
        DO $$ BEGIN
          CREATE ROLE {APP_ROLE} LOGIN;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        GRANT CONNECT ON DATABASE postgres TO {APP_ROLE};
        CREATE TABLE IF NOT EXISTS bg_kv (
          k text PRIMARY KEY,
          v text NOT NULL,
          seq bigint GENERATED BY DEFAULT AS IDENTITY
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON bg_kv TO {APP_ROLE};
        GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE};
        {PROBE_TABLE_DDL}
        """,
    )


def _assert_green_boot_fenced(green_leader: Node, budget: float = 60.0) -> None:
    """U1: green refuses app-role writes before any cutover entry exists.

    Polled: the fence's REVOKE cannot precede the role it targets, so it
    engages one reconcile pass after the role appears. Only the fence's own
    explicit denial counts as fenced; any other probe failure raises.
    """

    def fenced() -> bool | None:
        allowed = _psql_allowed(
            green_leader,
            "INSERT INTO bg_kv(k, v) VALUES ('boot-fence-probe', 'x') "
            "ON CONFLICT (k) DO UPDATE SET v = excluded.v;",
            APP_ROLE,
        )
        return None if allowed else True

    try:
        await_until(fenced, budget=budget, describe="green to deny the app role")
    except WaitTimeout as exc:
        raise ContractFailure(
            f"U1 violated: green kept accepting app-role writes before cutover ({exc})"
        ) from exc
    # A probe write that slipped in before the fence engaged is green-local
    # state blue never issued; leave none behind.
    _psql(green_leader, "DELETE FROM bg_kv WHERE k = 'boot-fence-probe';")
    console.print("green is fenced from boot: the app role got an explicit denial")


def _assert_arm_refused_without_authority(green_leader: Node) -> None:
    """U1 inversion: a cluster without write authority must refuse to arm.

    Arming claims authority for the local cluster, so one authenticated arm
    call on a boot-fenced recipient would self-elect it into authority while
    the donor still holds it. The refusal must be the authority guard's own,
    not some earlier validation — a probe refused for the wrong reason has
    not tested the guard.
    """
    r = _post(green_leader, "arm", {"target_cluster_id": BLUE_CLUSTER})
    if r.is_success:
        raise ContractFailure(
            f"U1 violated: green armed a cutover while blue holds write authority ({r.text})"
        )
    body: dict[str, Any] = {}
    with contextlib.suppress(ValueError):
        body = dict(r.json())
    if body.get("success") is True:
        raise ContractFailure(f"U1 violated: arm on green reported success in the body: {body}")
    message = str(body.get("message", r.text))
    if "authority" not in message:
        raise ContractFailure(
            f"arm on green was refused, but not by the authority guard: {r.status_code} {message}"
        )
    console.print(f"arm-guard OK: green refused to arm without authority ({r.status_code})")


def _seed_logical(blue_leader: Node, green_leader: Node) -> None:
    _psql(
        blue_leader,
        """
        SELECT pg_create_logical_replication_slot('bg_slot', 'pgoutput')
        WHERE NOT EXISTS (
          SELECT 1 FROM pg_replication_slots WHERE slot_name = 'bg_slot'
        );
        """,
    )
    _psql(
        blue_leader,
        """
        DO $$ BEGIN
          CREATE PUBLICATION bg_pub FOR TABLE bg_kv;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    blue_conn = (
        f"host={blue_leader.ip} port={topology.PG_INTERNAL_PORT} user={SUB_OWNER} dbname=postgres"
    )
    _psql(
        green_leader,
        f"""
        DO $$ BEGIN
          CREATE SUBSCRIPTION bg_sub
            CONNECTION '{blue_conn}'
            PUBLICATION bg_pub
            WITH (copy_data = true, create_slot = false, slot_name = 'bg_slot');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    _await_slot_confirms(blue_leader, _blue_flush_lsn(blue_leader), timeout=120.0)


def _blue_flush_lsn(blue_leader: Node) -> int:
    return _lsn_bytes(_psql(blue_leader, "SELECT pg_current_wal_flush_lsn()::text;"))


def _slot_confirmed_lsn(blue_leader: Node, slot: str = "bg_slot") -> int | None:
    """How far the subscriber has confirmed, read from blue's own catalog."""
    raw = _psql(
        blue_leader,
        f"SELECT COALESCE(confirmed_flush_lsn::text, '') "
        f"FROM pg_replication_slots WHERE slot_name = '{slot}';",
    )
    return _lsn_bytes(raw) if raw else None


def _await_slot_confirms(
    blue_leader: Node, target_lsn: int, timeout: float, slot: str = "bg_slot"
) -> int:
    """The drain gate. Row counts cannot answer it; an UPDATE moves the LSN only.

    `slot` differs by direction: the reverse drain reads green's reverse slot.
    """
    last: int | None = None

    def confirmed() -> int | None:
        nonlocal last
        last = _slot_confirmed_lsn(blue_leader, slot)
        return last if last is not None and last >= target_lsn else None

    try:
        return await_until(
            confirmed,
            budget=timeout,
            describe=f"slot {slot} on {blue_leader.service} to confirm {target_lsn}",
        )
    except WaitTimeout as exc:
        raise ContractFailure(
            f"U2: subscriber confirmed only {last} of the donor's {target_lsn} ({exc})"
        ) from exc


def _advance_sequences(green_leader: Node, blue_leader: Node) -> int:
    """Push green's sequence past every value blue issued (U3)."""
    blue_max = int(_psql(blue_leader, "SELECT COALESCE(max(seq), 0)::text FROM bg_kv;") or "0")
    target = blue_max + 1_000
    seq_name = _psql(
        green_leader,
        "SELECT COALESCE(pg_get_serial_sequence('bg_kv', 'seq'), '');",
    )
    if not seq_name:
        raise ContractFailure(
            "U3: green's bg_kv.seq owns no sequence, so setval would be a no-op "
            "and the sequence check would pass while blind"
        )
    _psql(green_leader, f"SELECT setval('{seq_name}', {target}, true);")
    return blue_max


def _assert_no_sequence_collision(green_leader: Node, blue_max: int) -> None:
    """U3: green must not issue a sequence value blue already used."""
    # Wrapped in a CTE so the result is a row set: a bare INSERT ... RETURNING
    # makes psql print the `INSERT 0 1` command tag alongside the value.
    issued = int(
        _psql(
            green_leader,
            "WITH probe AS ("
            "  INSERT INTO bg_kv(k, v) VALUES ('u3-probe', 'x') "
            "  ON CONFLICT (k) DO UPDATE SET v = excluded.v RETURNING seq"
            ") SELECT seq FROM probe;",
        )
    )
    if issued <= blue_max:
        raise ContractFailure(
            f"U3 violated: green issued seq {issued}, blue had already issued up to {blue_max}"
        )
    console.print(f"U3 OK: green issued {issued} > blue max {blue_max}")


def _assert_drain_complete(green_leader: Node, acked: set[str]) -> None:
    """U2: every key blue acked before SEAL is present on green."""
    if not acked:
        raise ContractFailure("U2 check is vacuous: no pre-seal writes were acked")
    present = set(
        _psql(
            green_leader,
            "SELECT string_agg(k, E'\\n') FROM bg_kv WHERE k LIKE 'pre-%' OR k LIKE 'stream-%';",
        ).splitlines()
    )
    missing = acked - present
    if missing:
        raise ContractFailure(
            f"U2 violated: {len(missing)} of {len(acked)} pre-seal acked writes "
            f"missing on green, e.g. {sorted(missing)[:5]}"
        )
    console.print(f"U2 OK: all {len(acked)} pre-seal acked writes present on green")


def _post(node: Node, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.post(
        f"{_mgmt(node)}/api/v1/admin/cutover/{path}",
        headers=_headers(),
        json=body or {},
        timeout=60.0,
    )


def _cutover(blue_leader: Node, green_leader: Node, blue: list[Node]) -> None:
    _post(blue_leader, "arm", {"target_cluster_id": GREEN_CLUSTER}).raise_for_status()

    # The server fences first and reads its own final LSN after; passing one
    # in from here would leave a window of acked writes past it.
    _post(blue_leader, "seal", {"app_roles": [APP_ROLE]}).raise_for_status()

    status = _cutover_status(blue_leader)
    final_lsn = status.get("sealed_final_lsn")
    if not isinstance(final_lsn, int):
        raise ContractFailure(f"SEAL recorded no final LSN: {status}")
    if status.get("holds_client_write_authority"):
        raise ContractFailure("blue still reports write authority after SEAL")

    # The fence is the contract, not the gateway hold: prove the app role
    # cannot write through blue's internal port either.
    if _psql_allowed(blue_leader, "INSERT INTO bg_kv(k,v) VALUES ('fence-probe','x');", APP_ROLE):
        raise ContractFailure("U1: app role still writes to blue's internal port after SEAL")
    console.print(f"SEAL OK: final_lsn={final_lsn}, app role fenced on the internal port")

    _await_slot_confirms(blue_leader, final_lsn, timeout=180.0)
    _post(blue_leader, "drain-observed").raise_for_status()

    blue_max = _advance_sequences(green_leader, blue_leader)

    # Reverse slot before green's fence lifts (U5): a slot created after the
    # first accepted write leaves a gap the reverse stream can never carry.
    _psql(
        green_leader,
        """
        DO $$ BEGIN
          CREATE PUBLICATION bg_reverse FOR TABLE bg_kv;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    _psql(
        green_leader,
        """
        SELECT pg_create_logical_replication_slot('bg_reverse_slot', 'pgoutput')
        WHERE NOT EXISTS (
          SELECT 1 FROM pg_replication_slots WHERE slot_name = 'bg_reverse_slot'
        );
        """,
    )

    _post(
        blue_leader,
        "transfer",
        {
            "target_cluster_id": GREEN_CLUSTER,
            "target_pg_addr": _pg_addr(green_leader),
            "reverse_slot_ready": True,
        },
    ).raise_for_status()

    epoch = int(_cutover_status(blue_leader)["epoch"])
    _receive_authority(green_leader, epoch, BLUE_CLUSTER, blue)
    _assert_minority_cannot_hand_over(green_leader, epoch, blue)
    _assert_authority_is_green_only(blue, green_leader, epoch)
    _assert_no_sequence_collision(green_leader, blue_max)


def _mgmt_addrs(nodes: list[Node]) -> list[str]:
    return [f"{n.ip}:{topology.MGMT_CONTAINER_PORT}" for n in nodes]


def _receive_authority(recipient: Node, epoch: int, donor_cluster: str, donors: list[Node]) -> None:
    """The recipient confirms the handoff against a majority of donor nodes.

    Not `accept_unverified_provenance`: that is the break-glass for an
    unreachable donor, and using it here would leave the verification path
    untested in the only case that runs it.
    """
    r = _post(
        recipient,
        "receive-authority",
        {
            "epoch": epoch,
            "from_cluster_id": donor_cluster,
            "donor_mgmt_addrs": _mgmt_addrs(donors),
        },
    )
    r.raise_for_status()


def _assert_minority_cannot_hand_over(recipient: Node, epoch: int, donors: list[Node]) -> None:
    """U4: one donor node's word is not a committed handoff.

    Takes the server's full donor-quorum budget: a minority now is not proof
    that a majority will not arrive, so the recipient waits before refusing.
    """
    r = _post(
        recipient,
        "receive-authority",
        {
            "epoch": epoch + 1,
            "from_cluster_id": BLUE_CLUSTER,
            "donor_mgmt_addrs": _mgmt_addrs(donors[:1]),
        },
    )
    if r.status_code == 200:
        raise ContractFailure("U4 violated: the recipient took authority on one donor node's word")
    console.print(f"U4 OK: a single donor node cannot hand over ({r.status_code})")


def _assert_authority_is_green_only(blue: list[Node], green_leader: Node, epoch: int) -> None:
    """U1: exactly one cluster reports write authority, and it is green."""
    for n in blue:
        st = _cutover_status(n)
        if st.get("holds_client_write_authority"):
            raise ContractFailure(f"U1 violated: {n.service} still claims authority: {st}")
    gs = _cutover_status(green_leader)
    if not gs.get("holds_client_write_authority"):
        raise ContractFailure(f"green did not take authority: {gs}")
    if int(gs.get("epoch", 0)) != epoch:
        raise ContractFailure(f"green epoch {gs.get('epoch')} != blue's {epoch}")
    console.print(f"U1 OK: authority is green at epoch {epoch}; no blue node claims it")


def _assert_relay_wrote_nothing_local(before: int, after: int, relayed: int) -> None:
    """U1: with no reverse subscription wired, the relay adds nothing to blue.

    Probed before `_wire_reverse`: once the reverse stream runs, blue legally
    grows by exactly the relayed rows — the same number a dual-writing gateway
    would add, so a post-wiring bound can never fail.
    """
    if after != before:
        raise ContractFailure(
            f"U1 violated: blue's own database changed by {after - before} rows "
            f"behind {relayed} relayed writes with no reverse subscription wired"
        )


def _await_reverse_arrival(blue_leader: Node, relayed: set[str], timeout: float = 90.0) -> None:
    """The reverse stream is live: green's rows actually reach blue through it."""
    missing: set[str] = set(relayed)

    def arrived() -> bool | None:
        nonlocal missing
        present = set(
            _psql(
                blue_leader,
                "SELECT string_agg(k, E'\\n') FROM bg_kv WHERE k LIKE 'post-%';",
            ).splitlines()
        )
        missing = relayed - present
        return True if not missing else None

    try:
        await_until(arrived, budget=timeout, describe="the reverse stream to reach blue")
    except WaitTimeout as exc:
        raise ContractFailure(
            f"rollback unavailable: {len(missing)} relayed rows never reached blue "
            f"through the reverse stream, e.g. {sorted(missing)[:5]} ({exc})"
        ) from exc


def _wire_reverse(blue_leader: Node, green_leader: Node) -> None:
    green_conn = (
        f"host={green_leader.ip} port={topology.PG_INTERNAL_PORT} user={SUB_OWNER} dbname=postgres"
    )
    _psql(
        blue_leader,
        f"""
        DO $$ BEGIN
          CREATE SUBSCRIPTION bg_reverse_sub
            CONNECTION '{green_conn}'
            PUBLICATION bg_reverse
            WITH (copy_data = false, create_slot = false,
                  slot_name = 'bg_reverse_slot');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )


def _rollback_available(blue_leader: Node) -> bool:
    st = _psql(
        blue_leader,
        "SELECT count(*)::text FROM pg_stat_subscription "
        "WHERE subname = 'bg_reverse_sub' AND pid IS NOT NULL;",
    )
    return st == "1"


def _write_burst(gateway_port: int, n: int, prefix: str, user: str = SUB_OWNER) -> set[str]:
    """Write `n` rows, returning only the keys the server acked."""
    acked: set[str] = set()
    with psycopg.connect(
        host="127.0.0.1",
        port=gateway_port,
        user=user,
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for i in range(n):
                key = f"{prefix}-{i}"
                cur.execute(
                    "INSERT INTO bg_kv(k, v) VALUES (%s, %s) "
                    "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                    (key, f"v-{i}"),
                )
                acked.add(key)
    return acked


def _run_prober() -> None:
    """U1 across all six internal ports, connected as the app role."""
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    result = ARTIFACT_DIR / "result.json"
    proc = subprocess.run(
        [
            "./testing/dual_writability_prober.py",
            "--topology",
            "bluegreen",
            "--duration",
            "15",
            "--user",
            APP_ROLE,
            # A run where nothing ever accepts is not evidence that at most
            # one node did.
            "--min-single-acceptance-rate",
            "0.5",
            "--json",
            str(result),
        ],
        cwd=PROJECT_ROOT,
        check=False,
        env={**os.environ},
    )
    if proc.returncode != 0:
        detail = result.read_text() if result.exists() else "no result file"
        raise ContractFailure(f"U1 prober rc={proc.returncode}: {detail}")
    console.print("U1 prober PASS across six ports as the app role")


def _kill_and_await_down(node: Node, budget: float = 30.0) -> None:
    """Kill a container and confirm the fault's effect: it is not running."""
    _compose("kill", node.service, check=True)

    def down() -> bool | None:
        state = _compose("ps", "-a", "--format", "{{.Service}} {{.State}}").stdout
        for line in state.splitlines():
            service, _, status = line.partition(" ")
            if service == node.service and status.strip().startswith("running"):
                return None
        return True

    try:
        await_until(down, budget=budget, describe=f"{node.service} to be down after the kill")
    except WaitTimeout as exc:
        raise ContractFailure(f"the kill did not land: {exc}") from exc


def _await_survivor_leader(nodes: list[Node], dead: Node, budget: float = 90.0) -> Node:
    """The survivors' own view has moved past the killed leader."""
    survivors = [n for n in nodes if n.service != dead.service]

    def elected() -> Node | None:
        for n in survivors:
            try:
                r = httpx.get(f"{_mgmt(n)}/api/v1/cluster/leader", timeout=2.0)
            except httpx.HTTPError:
                continue
            if r.status_code != 200:
                continue
            lid = r.json().get("leader_id")
            if lid is None or int(lid) == dead.node_id:
                continue
            for cand in survivors:
                if cand.node_id == int(lid):
                    return cand
        return None

    try:
        return await_until(
            elected,
            budget=budget,
            describe=f"a leader among the survivors of {dead.service}",
        )
    except WaitTimeout as exc:
        raise ContractFailure(f"no survivor took over after {dead.service} died: {exc}") from exc


def _assert_green_stays_fenced(green_leader: Node, window_sec: float, why: str) -> None:
    """U4: green refuses app-role writes across the whole window.

    Elapsed time is the point here: green must not take authority on its own
    at any moment while the donor is down, so this probes continuously
    instead of once after a blind sleep.
    """
    deadline = time.monotonic() + window_sec
    probes = 0
    while time.monotonic() < deadline:
        if _psql_allowed(
            green_leader,
            "INSERT INTO bg_kv(k, v) VALUES ('chaos', 'x') "
            "ON CONFLICT (k) DO UPDATE SET v = excluded.v;",
            APP_ROLE,
        ):
            raise ContractFailure(f"U4 violated: green accepted an app-role write {why}")
        probes += 1
        time.sleep(1.0)
    console.print(f"green refused {probes} app-role writes over {window_sec:g}s {why}")


def _chaos_arm_seal(blue_leader: Node, green_leader: Node, blue: list[Node]) -> None:
    _post(blue_leader, "arm", {"target_cluster_id": GREEN_CLUSTER}).raise_for_status()
    _kill_and_await_down(blue_leader)
    survivor = _await_survivor_leader(blue, dead=blue_leader)
    _assert_green_stays_fenced(green_leader, 10.0, "with blue's leader dead after ARM")
    console.print(f"chaos arm-seal OK: kill landed, {survivor.service} took blue, green read-only")


def _chaos_seal_transfer(blue_leader: Node, green_leader: Node, blue: list[Node]) -> None:
    _post(blue_leader, "arm", {"target_cluster_id": GREEN_CLUSTER}).raise_for_status()
    _post(blue_leader, "seal", {"app_roles": [APP_ROLE]}).raise_for_status()
    _kill_and_await_down(blue_leader)
    survivor = _await_survivor_leader(blue, dead=blue_leader)
    _assert_green_stays_fenced(green_leader, 10.0, "with blue's leader dead after SEAL")
    console.print(
        f"chaos seal-transfer OK: kill landed, {survivor.service} took blue, green read-only"
    )


def _chaos_mid_drain(blue_leader: Node, green_leader: Node, _blue: list[Node]) -> None:
    _post(blue_leader, "arm", {"target_cluster_id": GREEN_CLUSTER}).raise_for_status()
    _post(blue_leader, "seal", {"app_roles": [APP_ROLE]}).raise_for_status()
    _kill_and_await_down(green_leader)
    r = _post(blue_leader, "abort", {"app_roles": [APP_ROLE]})
    r.raise_for_status()
    st = _cutover_status(blue_leader)
    if st.get("phase") != "aborted":
        raise ContractFailure(f"abort did not unseal blue: {st}")
    if not st.get("holds_client_write_authority"):
        raise ContractFailure(f"blue did not reclaim authority after abort: {st}")
    if int(st.get("epoch", 0)) < 1:
        raise ContractFailure(f"abort must unseal into a new epoch: {st}")

    def unsealed() -> bool | None:
        allowed = _psql_allowed(
            blue_leader,
            "INSERT INTO bg_kv(k, v) VALUES ('unsealed', 'x') "
            "ON CONFLICT (k) DO UPDATE SET v = excluded.v;",
            APP_ROLE,
        )
        return True if allowed else None

    try:
        await_until(unsealed, budget=60.0, describe="blue to accept app-role writes again")
    except WaitTimeout as exc:
        raise ContractFailure(
            f"abort reported success but blue never accepted a write again: {exc}"
        ) from exc
    console.print(
        f"chaos mid-drain OK: green's leader confirmed dead, blue unsealed, epoch {st['epoch']}"
    )


def _voter_ids(node: Node) -> list[int]:
    """Raft voters as `node` sees them. Learners cannot carry a quorum."""
    r = httpx.get(f"{_mgmt(node)}/api/v1/cluster/members", timeout=10.0)
    r.raise_for_status()
    members = r.json().get("members") or []
    return [int(m["node_id"]) for m in members if str(m.get("role", "")).lower() == "voter"]


def _assert_three_voters(node: Node) -> None:
    """Stopping two only removes quorum if all three are voters, not learners."""
    voters = _voter_ids(node)
    if len(voters) != 3:
        raise ContractFailure(
            f"blue has {len(voters)} voters {voters}, not 3; stopping two would "
            "not remove quorum and the case cannot test what it claims"
        )


def _assert_stopped(nodes: list[Node]) -> None:
    """With the containers confirmed down, no majority can ack and timing is moot."""
    for n in nodes:
        state = _compose("ps", "-a", "--format", "{{.Service}} {{.State}}").stdout
        for line in state.splitlines():
            service, _, status = line.partition(" ")
            if service == n.service and status.strip().startswith("running"):
                raise ContractFailure(f"{n.service} still running after compose stop: {status}")


def _chaos_blue_quorum(blue_leader: Node, green_leader: Node, blue: list[Node]) -> None:
    _post(blue_leader, "arm", {"target_cluster_id": GREEN_CLUSTER}).raise_for_status()
    _post(blue_leader, "seal", {"app_roles": [APP_ROLE]}).raise_for_status()
    followers = [n for n in blue if n.service != blue_leader.service]
    _assert_three_voters(blue_leader)
    stopped = followers[:2]
    for f in stopped:
        _compose("stop", f.service, check=True)
    _assert_stopped(stopped)

    body = {
        "target_cluster_id": GREEN_CLUSTER,
        "target_pg_addr": _pg_addr(green_leader),
        "accept_write_loss": True,
    }
    r = _post(blue_leader, "force-transfer", body)
    if r.status_code == 200:
        raise ContractFailure(
            "force-transfer succeeded without blue quorum: it commits to blue's "
            "log, so it must fail closed until quorum returns"
        )
    if _psql_allowed(green_leader, "INSERT INTO bg_kv(k,v) VALUES ('chaos','x');", APP_ROLE):
        raise ContractFailure("U4 violated: green wrote while blue had no quorum")
    console.print(
        f"chaos blue-quorum: force-transfer failed closed ({r.status_code}), green read-only"
    )

    for f in followers[:2]:
        _compose("start", f.service)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if _post(blue_leader, "force-transfer", body).status_code == 200:
            st = _cutover_status(blue_leader)
            if not st.get("transfer_forced"):
                raise ContractFailure(f"forced transfer not recorded as forced: {st}")
            console.print("chaos blue-quorum OK: forced transfer recorded, write loss accepted")
            return
        time.sleep(3)
    raise ContractFailure("force-transfer never succeeded after quorum returned")


CHAOS: Final[dict[str, Callable[[Node, Node, list[Node]], None]]] = {
    "arm-seal": _chaos_arm_seal,
    "seal-transfer": _chaos_seal_transfer,
    "mid-drain": _chaos_mid_drain,
    "blue-quorum": _chaos_blue_quorum,
}


def _rollback_drill(
    blue_leader: Node, green_leader: Node, blue: list[Node], green: list[Node]
) -> None:
    """U5: reverse the protocol, green -> blue, and prove the window held."""
    green_acked = _write_burst(green_leader.gateway_port, 5, "rb")

    _post(green_leader, "arm", {"target_cluster_id": BLUE_CLUSTER}).raise_for_status()
    _post(green_leader, "seal", {"app_roles": [APP_ROLE]}).raise_for_status()
    st = _cutover_status(green_leader)
    final_lsn = st.get("sealed_final_lsn")
    if not isinstance(final_lsn, int):
        raise ContractFailure(f"reverse SEAL recorded no final LSN: {st}")

    _await_slot_confirms(green_leader, final_lsn, timeout=120.0, slot="bg_reverse_slot")
    _post(green_leader, "drain-observed").raise_for_status()
    _post(
        green_leader,
        "transfer",
        {
            "target_cluster_id": BLUE_CLUSTER,
            "target_pg_addr": _pg_addr(blue_leader),
            "reverse_slot_ready": True,
        },
    ).raise_for_status()
    epoch = int(_cutover_status(green_leader)["epoch"])
    _receive_authority(blue_leader, epoch, GREEN_CLUSTER, green)

    present = set(
        _psql(
            blue_leader,
            "SELECT string_agg(k, E'\\n') FROM bg_kv WHERE k LIKE 'rb-%';",
        ).splitlines()
    )
    missing = green_acked - present
    if missing:
        raise ContractFailure(
            f"U5 violated: {len(missing)} writes green acked never reached blue: {sorted(missing)}"
        )
    gs = _cutover_status(green_leader)
    if gs.get("holds_client_write_authority"):
        raise ContractFailure(f"U1 violated: green kept authority after rollback: {gs}")
    holders = [n.service for n in blue if _cutover_status(n).get("holds_client_write_authority")]
    if len(holders) != len(blue):
        raise ContractFailure(
            f"U1 violated: only {holders} of {[n.service for n in blue]} took authority back"
        )
    console.print(
        f"rollback drill OK: all {len(green_acked)} green writes on blue, authority back at {epoch}"
    )


@app.command()
def main(
    chaos: str | None = typer.Option(None, help=" | ".join(CHAOS)),
    rollback_drill: bool = typer.Option(False, "--rollback-drill"),
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
) -> None:
    if chaos is not None and chaos not in CHAOS:
        raise SystemExit(f"unknown --chaos {chaos!r}; expected one of {sorted(CHAOS)}")

    os.environ["COMPOSE_FILE"] = "docker-compose.bluegreen.yml"
    if not no_up:
        _compose("down", "-v", "--remove-orphans")
        _compose("up", "-d", "--remove-orphans", check=True)

    topo = load()
    blue, green = _blue_green(topo)
    console.print(f"topology: blue={[n.service for n in blue]} green={[n.service for n in green]}")

    for n in (blue[0], green[0]):
        _wait_leader(n, timeout=180)
    blue_leader = _leader_node(blue)
    green_leader = _leader_node(green)

    def _maj(node: Node) -> int:
        return int(_psql(node, "SHOW server_version_num;").strip()) // 10_000

    bm, gm = _maj(blue_leader), _maj(green_leader)
    if bm != BLUE_MAJOR or gm != GREEN_MAJOR:
        raise SystemExit(
            f"anti-vacuity: expected blue@{BLUE_MAJOR} green@{GREEN_MAJOR}, "
            f"got blue@{bm} green@{gm}"
        )
    console.print(f"leaders: blue={blue_leader.service}@{bm} green={green_leader.service}@{gm}")

    _setup_schema(blue_leader)
    _setup_green_schema(green_leader)

    # Green must refuse client writes before any cutover entry exists: a
    # second Raft group electing itself into authority is the failure this
    # whole track is built to prevent.
    _assert_green_boot_fenced(green_leader)
    _assert_arm_refused_without_authority(green_leader)

    acked = _write_burst(blue_leader.gateway_port, 20, "pre")
    _seed_logical(blue_leader, green_leader)
    acked |= _write_burst(blue_leader.gateway_port, 30, "stream")

    if chaos:
        CHAOS[chaos](blue_leader, green_leader, blue)
        console.print(f"chaos={chaos} passed")
        return

    _cutover(blue_leader, green_leader, blue)
    _assert_drain_complete(green_leader, acked)

    status = _cutover_status(blue_leader)
    console.print("cutover status:", json.dumps(status, indent=2))
    if status.get("phase") != "transferred":
        raise ContractFailure(f"expected transferred, got {status}")

    # Blue's gateway is a relay now: these writes must land on green, and
    # blue's own database must not grow at all behind them. This runs before
    # reverse wiring, where zero growth is the only legal answer.
    blue_rows_before = int(_psql(blue_leader, "SELECT count(*)::text FROM bg_kv;"))
    relayed = _write_burst(blue_leader.gateway_port, 10, "post")
    on_green = int(_psql(green_leader, "SELECT count(*)::text FROM bg_kv WHERE k LIKE 'post-%';"))
    if on_green != len(relayed):
        raise ContractFailure(
            f"blue's gateway relayed {on_green} of {len(relayed)} writes to green"
        )
    blue_rows_after = int(_psql(blue_leader, "SELECT count(*)::text FROM bg_kv;"))
    _assert_relay_wrote_nothing_local(blue_rows_before, blue_rows_after, len(relayed))
    console.print(f"relay OK: {on_green} writes through blue's gateway landed on green only")

    _wire_reverse(blue_leader, green_leader)
    if not _rollback_available(blue_leader):
        raise ContractFailure("reverse subscription not active — rollback unavailable")
    _await_reverse_arrival(blue_leader, relayed)
    console.print("reverse stream OK: green's rows reach blue")

    if rollback_drill:
        _rollback_drill(blue_leader, green_leader, blue, green)

    _run_prober()
    console.print("bluegreen cutover e2e OK")


if __name__ == "__main__":
    app()
