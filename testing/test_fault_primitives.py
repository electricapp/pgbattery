#!/usr/bin/env -S uv run --project testing python
"""Unit tests for fault_primitives.

These tests do NOT require docker. Every fixture below is real output captured
from the compose cluster (``tc``, ``ps``, ``df``, ``psql``), and every primitive
exercised end-to-end runs against a scripted command runner installed with
``set_command_runner``.

The point of most of them is the red half of red-green: an effect verifier must
FAIL when the fault did not land. A rule that is installed but matched zero
packets, a netem qdisc that never appeared, a SIGSTOP that left the process in
state ``S``, a fill that left room for another WAL segment, a libfaketime write
that moved nothing — each of those is asserted to raise, because the whole
reason this module exists is that a fault which cannot fail is worse than no
test at all.

Run with:
    uv run --project testing python testing/test_fault_primitives.py
"""

from __future__ import annotations

import os
import re
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest import mock

import fault_primitives as fp
from fault_primitives import (
    DEFAULT_COMPOSE_PROJECT,
    Aim,
    Channel,
    CommandResult,
    Direction,
    DiskUsage,
    FaultEffectNotObserved,
    FaultInjectionError,
    FaultPreconditionError,
    FilterMatch,
    LeaseBoundarySkew,
    NetemState,
    ProcessInfo,
    cluster_network,
    compose_project,
    container_id,
    container_networks_cmd,
    count_prio_for,
    curl_probe_cmd,
    df_cmd,
    disk_full_during_wal,
    drop_prio_for,
    egress_drop_cmds,
    faketime_offset_literal,
    faketime_write_cmd,
    fsync_stall,
    ingress_filter_add_cmd,
    ingress_filter_del_cmd,
    ingress_qdisc_add_cmd,
    ip_to_u32_hex,
    netem_add_cmd,
    netem_del_cmd,
    network_detached,
    parse_container_networks,
    parse_curl_seconds,
    parse_df,
    parse_netem,
    parse_port_drop_packets,
    parse_port_drop_rule,
    parse_ps,
    parse_rust_duration_const_ms,
    parse_rust_u64_const,
    parse_size_literal,
    parse_tc_filters,
    partition_channel,
    partition_lossy,
    ps_cmd,
    read_system_timings,
    scrub,
    select_pg_processes,
    set_command_runner,
    set_event_sink,
    sigstop_checkpointer,
    sweep_around,
    verify_added_latency,
    verify_attached,
    verify_bounded_filesystem,
    verify_clock_offset,
    verify_detached,
    verify_filter_absent,
    verify_filter_present,
    verify_netem_absent,
    verify_netem_applied,
    verify_out_of_space,
    verify_packets_matched,
    verify_probe_blackholed,
    verify_probe_reachable,
    verify_processes_stopped,
    verify_scope_unaffected,
    verify_space_restored,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — verbatim output from the compose cluster
# ─────────────────────────────────────────────────────────────────────────────

NETEM_QDISC = """\
qdisc netem 8001: root refcnt 11 limit 1000 delay 120ms  30ms loss 25% seed 11386401599413554931
 Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0
"""

NETEM_QDISC_WITH_DROPS = """\
qdisc netem 10: parent 1:1 limit 1000 loss 100% seed 12787007573496287065
 Sent 0 bytes 0 pkt (dropped 7, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0
"""

NO_QDISC = "qdisc noqueue 0: root refcnt 2\n"

INGRESS_DROP_FILTER_COLD = """\
filter protocol ip pref 112 u32 chain 0
filter protocol ip pref 112 u32 chain 0 fh 800: ht divisor 1
filter protocol ip pref 112 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 \
terminal flowid not_in_hw (rule hit 0 success 0)
  match ac1c000c/ffffffff at 12 (success 0 )
\taction order 1: gact action drop
"""

INGRESS_DROP_FILTER_HOT = """\
filter protocol ip pref 112 u32 chain 0
filter protocol ip pref 112 u32 chain 0 fh 800: ht divisor 1
filter protocol ip pref 112 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 \
terminal flowid not_in_hw (rule hit 50 success 25)
  match ac1c000c/ffffffff at 12 (success 25 )
\taction order 1: gact action drop
"""

EGRESS_DROP_FILTER_HOT = """\
filter protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 \
flowid 1:1 not_in_hw (rule hit 40 success 7)
  match ac1c000c/ffffffff at 16 (success 7 )
"""

PS_LEADER = """\
   20 S    /usr/lib/postgresql/18/bin/postgres -D /var/lib/postgresql/data -p 5434
   22 Ss   postgres: io worker 0
   25 Ss   postgres: checkpointer
   26 Ss   postgres: background writer
   28 Ss   postgres: walwriter
   29 Ss   postgres: autovacuum launcher
   34 Ss   postgres: postgres postgres [local] idle
   56 Ss   postgres: walsender postgres 172.28.0.12(48426) streaming 0/4051230
"""

PS_STANDBY = """\
   19 S    /usr/lib/postgresql/18/bin/postgres -D /var/lib/postgresql/data -p 5434
   24 Ss   postgres: checkpointer
   25 Ss   postgres: background writer
   26 Ss   postgres: startup recovering 000000010000000000000004
   59 Ss   postgres: walreceiver streaming 0/4051230
"""

PS_CHECKPOINTER_STOPPED = """\
   42 S    /usr/lib/postgresql/18/bin/postgres -D /var/lib/postgresql/data -p 5434
   47 Ts   postgres: checkpointer
   48 Ss   postgres: walwriter
"""

PS_ALL_STOPPED = """\
   42 T    /usr/lib/postgresql/18/bin/postgres -D /var/lib/postgresql/data -p 5434
   47 Ts   postgres: checkpointer
"""

DF_UNBOUNDED = """\
Filesystem     1024-blocks     Used Available Capacity Mounted on
/dev/vda1        474044488 44094528 405796312      10% /var/lib/postgresql
"""

DF_BOUNDED_EMPTY = """\
Filesystem     1024-blocks     Used Available Capacity Mounted on
tmpfs              4194304    90112   4104192       3% /var/lib/postgresql
"""

DF_BOUNDED_FULL = """\
Filesystem     1024-blocks     Used Available Capacity Mounted on
tmpfs              4194304  4186112      8192      99% /var/lib/postgresql
"""

WAL_SEGMENT_BYTES = 16 * 1024**2


# ─────────────────────────────────────────────────────────────────────────────
# Scripted command runner
# ─────────────────────────────────────────────────────────────────────────────


class ScriptedRunner:
    """Answers shell commands by matching substrings, recording every call.

    Rules are ``(needle, CommandResult)`` pairs, checked in order; the first
    match wins. An unmatched command returns rc 0 with empty output, which is
    what a successful ``tc``/``kill``/``rm`` looks like.
    """

    def __init__(self, rules: Sequence[tuple[str, CommandResult]]) -> None:
        self.rules = list(rules)
        self.calls: list[str] = []

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        self.calls.append(cmd)
        for needle, result in self.rules:
            if needle in cmd:
                return result
        return CommandResult(0, "", "")

    def matching(self, needle: str) -> list[str]:
        return [call for call in self.calls if needle in call]


def ok(stdout: str = "") -> CommandResult:
    return CommandResult(0, stdout, "")


def fail(stderr: str, rc: int = 1) -> CommandResult:
    return CommandResult(rc, "", stderr)


class RunnerFixture(unittest.TestCase):
    """Installs a scripted runner and a capturing event sink for the test."""

    def install(self, runner: ScriptedRunner) -> ScriptedRunner:
        previous_runner = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous_runner)
        self.events: list[dict[str, object]] = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)
        return runner


# ─────────────────────────────────────────────────────────────────────────────
# Constant derivation from the Rust source
# ─────────────────────────────────────────────────────────────────────────────


class RustConstantTests(unittest.TestCase):
    def test_u64_const_with_underscores(self) -> None:
        source = "pub const QUORUM_TIMEOUT_MS: u64 = 1_000;"
        self.assertEqual(parse_rust_u64_const(source, "QUORUM_TIMEOUT_MS"), 1000)

    def test_u64_const_missing_raises_actionably(self) -> None:
        with self.assertRaises(FaultPreconditionError) as caught:
            parse_rust_u64_const("pub const OTHER: u64 = 1;", "QUORUM_TIMEOUT_MS")
        self.assertIn("QUORUM_TIMEOUT_MS", str(caught.exception))
        self.assertIn("stale default", str(caught.exception))

    def test_duration_const_secs_and_millis(self) -> None:
        secs = "pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(2);"
        millis = "pub const LEASE_CHECK_INTERVAL: Duration = Duration::from_millis(100);"
        self.assertEqual(parse_rust_duration_const_ms(secs, "DEFAULT_LEASE_DURATION"), 2000)
        self.assertEqual(parse_rust_duration_const_ms(millis, "LEASE_CHECK_INTERVAL"), 100)

    def test_duration_const_missing_raises(self) -> None:
        with self.assertRaises(FaultPreconditionError):
            parse_rust_duration_const_ms("nothing here", "DEFAULT_LEASE_DURATION")


class SystemTimingsTests(unittest.TestCase):
    """Reads the real repository, so a retuned constant surfaces here."""

    def setUp(self) -> None:
        self.timings = read_system_timings(REPO_ROOT)

    def test_lease_and_raft_constants(self) -> None:
        self.assertEqual(self.timings.lease_duration_ms, 2000)
        self.assertEqual(self.timings.lease_check_interval_ms, 100)
        self.assertEqual(self.timings.election_timeout_ms, 1000)
        self.assertEqual(self.timings.heartbeat_interval_ms, 250)
        self.assertEqual(self.timings.quorum_timeout_ms, 1000)
        self.assertEqual(self.timings.metrics_watchdog_timeout_ms, 1500)
        self.assertEqual(self.timings.lsn_staleness_threshold_ms, 30_000)
        self.assertEqual(self.timings.leadership_transfer_lease_safety_ms, 100)

    def test_election_timeout_comes_from_the_running_config(self) -> None:
        # config/node1.toml pins what the compose cluster actually runs, so it
        # wins over the Rust default. Fault timings must reflect the live value.
        self.assertEqual(self.timings.election_timeout_source, "config/node1.toml")

    def test_holddown_equals_lease_and_outlives_an_election(self) -> None:
        # The relationship the clock-skew primitive depends on: an election can
        # be won before the deposed leader's lease expires, so promotion is held
        # down for one full lease. Were this to invert, the primitive would be
        # aiming at a window that no longer exists.
        self.assertEqual(self.timings.promotion_holddown_ms, self.timings.lease_duration_ms)
        self.assertGreater(self.timings.lease_duration_ms, self.timings.election_timeout_ms)
        self.assertEqual(self.timings.worst_case_election_ms, 2000)

    def test_missing_repo_root_raises(self) -> None:
        with self.assertRaises(FaultPreconditionError):
            read_system_timings(Path("/nonexistent/pgbattery"))


class SweepTests(unittest.TestCase):
    def test_sweep_straddles_the_boundary(self) -> None:
        self.assertEqual(sweep_around(2000), [1000, 1800, 2000, 2200, 4000])

    def test_sweep_is_deduped_and_sorted(self) -> None:
        self.assertEqual(sweep_around(1000, (1.0, 1.0, 0.5)), [500, 1000])

    def test_sweep_rejects_nonpositive_boundary(self) -> None:
        with self.assertRaises(ValueError):
            sweep_around(0)


# ─────────────────────────────────────────────────────────────────────────────
# Command construction
# ─────────────────────────────────────────────────────────────────────────────


class AddressEncodingTests(unittest.TestCase):
    def test_ip_to_u32_hex_matches_what_tc_prints(self) -> None:
        # `tc filter show` renders 172.28.0.12 as `match ac1c000c/ffffffff`.
        self.assertEqual(ip_to_u32_hex("172.28.0.12"), "ac1c000c")
        self.assertEqual(ip_to_u32_hex("172.28.0.11"), "ac1c000b")

    def test_ip_to_u32_hex_rejects_junk(self) -> None:
        for bad in ("172.28.0", "172.28.0.999", "not-an-ip", "172.28.0.x"):
            with self.assertRaises(ValueError):
                ip_to_u32_hex(bad)

    def test_drop_and_count_priorities_never_collide(self) -> None:
        ips = [f"172.28.0.1{n}" for n in range(1, 5)]
        drops = [drop_prio_for(ip) for ip in ips]
        counts = [count_prio_for(ip) for ip in ips]
        self.assertEqual(len(set(drops)), len(ips))
        self.assertEqual(len(set(counts)), len(ips))
        self.assertEqual(set(drops) & set(counts), set())

    def test_priorities_are_deterministic(self) -> None:
        self.assertEqual(drop_prio_for("172.28.0.12"), drop_prio_for("172.28.0.12"))


class CommandConstructionTests(unittest.TestCase):
    def test_netem_combines_loss_and_delay(self) -> None:
        cmd = netem_add_cmd(delay_ms=200, jitter_ms=50, loss_pct=30)
        self.assertEqual(cmd, "tc qdisc add dev eth0 root netem delay 200ms 50ms loss 30%")

    def test_netem_delay_only_and_loss_only(self) -> None:
        self.assertEqual(
            netem_add_cmd(delay_ms=250, jitter_ms=0, loss_pct=0),
            "tc qdisc add dev eth0 root netem delay 250ms",
        )
        self.assertEqual(
            netem_add_cmd(delay_ms=0, jitter_ms=0, loss_pct=30),
            "tc qdisc add dev eth0 root netem loss 30%",
        )

    def test_netem_needs_at_least_one_effect(self) -> None:
        with self.assertRaises(ValueError):
            netem_add_cmd(delay_ms=0, jitter_ms=0, loss_pct=0)

    def test_netem_add_does_not_silently_clobber_an_existing_qdisc(self) -> None:
        # No `qdisc del` prefix: if another primitive owns the interface, tc
        # fails with "Exclusivity flag on" and the caller finds out.
        self.assertNotIn("qdisc del", netem_add_cmd(delay_ms=100, jitter_ms=0, loss_pct=0))

    def test_ingress_filter_actions(self) -> None:
        drop = ingress_filter_add_cmd("172.28.0.12", prio=112, action="drop")
        self.assertIn("parent ffff:", drop)
        self.assertIn("match ip src 172.28.0.12/32", drop)
        self.assertTrue(drop.endswith("action drop"))
        self.assertTrue(
            ingress_filter_add_cmd("172.28.0.12", prio=212, action="pass").endswith("action pass")
        )

    def test_ingress_filter_rejects_unknown_action(self) -> None:
        with self.assertRaises(ValueError):
            ingress_filter_add_cmd("172.28.0.12", prio=112, action="reject")

    def test_ingress_filter_del_targets_one_priority(self) -> None:
        # Deleting by priority rather than flushing the chain is what keeps two
        # concurrent partitions on one container from healing each other.
        self.assertEqual(
            ingress_filter_del_cmd(prio=112),
            "tc filter del dev eth0 parent ffff: protocol ip prio 112 u32",
        )

    def test_egress_drop_builds_prio_band_and_dst_filter(self) -> None:
        cmds = egress_drop_cmds("172.28.0.12")
        self.assertEqual(len(cmds), 3)
        self.assertIn("root handle 1: prio bands 3", cmds[0])
        self.assertIn("netem loss 100%", cmds[1])
        self.assertIn("match ip dst 172.28.0.12/32", cmds[2])
        self.assertIn("flowid 1:1", cmds[2])

    def test_shell_helpers_are_stable(self) -> None:
        self.assertEqual(netem_del_cmd(), "tc qdisc del dev eth0 root")
        self.assertEqual(ingress_qdisc_add_cmd(), "tc qdisc add dev eth0 handle ffff: ingress")
        self.assertEqual(ps_cmd(), "ps -eo pid=,stat=,args=")
        self.assertEqual(df_cmd("/var/lib/postgresql"), "df -k -P /var/lib/postgresql")
        self.assertIn("%{time_total}", curl_probe_cmd("172.28.0.12"))


class FaketimeLiteralTests(unittest.TestCase):
    """The measured hazard: libfaketime reads `m` as minutes.

    Writing `+250ms` shifts the clock by 250 MINUTES (measured on the cluster:
    a +250ms literal moved the container clock 15,000,000 ms). Only fractional
    seconds are safe, so the builder must never emit an `ms` suffix.
    """

    def test_never_emits_a_minute_suffix(self) -> None:
        for offset in (1, 100, 250, 999, 1000, 2000, 2500, 123_456):
            literal = faketime_offset_literal(offset)
            self.assertNotIn("ms", literal)
            self.assertTrue(literal.endswith("s"))

    def test_sub_second_offsets_are_fractional_seconds(self) -> None:
        self.assertEqual(faketime_offset_literal(250), "+0.250s")
        self.assertEqual(faketime_offset_literal(100), "+0.100s")

    def test_lease_sized_offsets(self) -> None:
        self.assertEqual(faketime_offset_literal(2000), "+2.000s")
        self.assertEqual(faketime_offset_literal(2500), "+2.500s")

    def test_negative_and_zero(self) -> None:
        self.assertEqual(faketime_offset_literal(-100), "-0.100s")
        self.assertEqual(faketime_offset_literal(0), "+0.000s")

    def test_write_command_targets_the_timestamp_file(self) -> None:
        self.assertEqual(faketime_write_cmd(2000), "echo '+2.000s' > /tmp/faketime")


# ─────────────────────────────────────────────────────────────────────────────
# Output parsing
# ─────────────────────────────────────────────────────────────────────────────


class NetemParsingTests(unittest.TestCase):
    def test_parses_delay_jitter_and_loss(self) -> None:
        state = parse_netem(NETEM_QDISC)
        assert state is not None
        self.assertEqual(state.delay_ms, 120.0)
        self.assertEqual(state.jitter_ms, 30.0)
        self.assertEqual(state.loss_pct, 25.0)
        self.assertEqual(state.dropped_packets, 0)

    def test_parses_drop_counter(self) -> None:
        state = parse_netem(NETEM_QDISC_WITH_DROPS)
        assert state is not None
        self.assertEqual(state.loss_pct, 100.0)
        self.assertEqual(state.dropped_packets, 7)

    def test_absent_netem_is_none(self) -> None:
        self.assertIsNone(parse_netem(NO_QDISC))
        self.assertIsNone(parse_netem(""))

    def test_normalises_time_units(self) -> None:
        state = parse_netem("qdisc netem 1: root limit 1000 delay 500us")
        assert state is not None
        self.assertAlmostEqual(state.delay_ms, 0.5)
        state = parse_netem("qdisc netem 1: root limit 1000 delay 1.5s")
        assert state is not None
        self.assertAlmostEqual(state.delay_ms, 1500.0)


class FilterParsingTests(unittest.TestCase):
    def test_parses_ingress_drop_filter_with_counters(self) -> None:
        matches = parse_tc_filters(INGRESS_DROP_FILTER_HOT)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].hex_key, "ac1c000c")
        self.assertEqual(matches[0].prio, 112)
        self.assertEqual(matches[0].offset, 12)
        self.assertEqual(matches[0].rule_hits, 50)
        self.assertEqual(matches[0].match_success, 25)

    def test_parses_egress_filter_at_destination_offset(self) -> None:
        matches = parse_tc_filters(EGRESS_DROP_FILTER_HOT)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].offset, 16)
        self.assertEqual(matches[0].prio, 1)
        self.assertEqual(matches[0].match_success, 7)

    def test_empty_dump_yields_nothing(self) -> None:
        self.assertEqual(parse_tc_filters(""), [])


class ProcessParsingTests(unittest.TestCase):
    def test_parses_state_and_title(self) -> None:
        processes = parse_ps(PS_LEADER)
        by_pid = {process.pid: process for process in processes}
        self.assertEqual(by_pid[25].pg_title, "checkpointer")
        self.assertEqual(by_pid[25].state, "S")
        self.assertFalse(by_pid[25].is_stopped)
        self.assertIsNone(by_pid[20].pg_title)

    def test_stopped_state_is_the_first_stat_character(self) -> None:
        # `ps` reports `Ts` for a session-leading stopped process.
        processes = parse_ps(PS_CHECKPOINTER_STOPPED)
        by_pid = {process.pid: process for process in processes}
        self.assertEqual(by_pid[47].state, "T")
        self.assertTrue(by_pid[47].is_stopped)
        self.assertFalse(by_pid[48].is_stopped)

    def test_selects_wal_durability_processes_on_a_leader(self) -> None:
        selected = select_pg_processes(
            parse_ps(PS_LEADER), ("checkpointer", "walwriter", "background writer", "walreceiver")
        )
        self.assertEqual(
            sorted(process.pg_title or "" for process in selected),
            ["background writer", "checkpointer", "walwriter"],
        )

    def test_selects_walreceiver_on_a_standby_by_title_prefix(self) -> None:
        # PG appends state to the title: `walreceiver streaming 0/4051230`.
        selected = select_pg_processes(parse_ps(PS_STANDBY), ("walreceiver",))
        self.assertEqual([process.pid for process in selected], [59])

    def test_does_not_select_unrelated_processes(self) -> None:
        selected = select_pg_processes(parse_ps(PS_LEADER), ("checkpointer",))
        self.assertEqual([process.pid for process in selected], [25])


class DiskParsingTests(unittest.TestCase):
    def test_parses_df_columns(self) -> None:
        usage = parse_df(DF_UNBOUNDED)
        self.assertEqual(usage.filesystem, "/dev/vda1")
        self.assertEqual(usage.total_kb, 474_044_488)
        self.assertEqual(usage.avail_kb, 405_796_312)
        self.assertEqual(usage.mount, "/var/lib/postgresql")

    def test_parses_bounded_tmpfs(self) -> None:
        usage = parse_df(DF_BOUNDED_EMPTY)
        self.assertEqual(usage.total_bytes, 4 * 1024**3)

    def test_unparseable_df_raises(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            parse_df("df: /nope: No such file or directory")

    def test_parses_pg_size_settings(self) -> None:
        self.assertEqual(parse_size_literal("16MB"), WAL_SEGMENT_BYTES)
        self.assertEqual(parse_size_literal("8kB"), 8192)
        self.assertEqual(parse_size_literal("1024"), 1024)

    def test_unknown_size_unit_raises(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            parse_size_literal("16 furlongs")


class CurlParsingTests(unittest.TestCase):
    def test_parses_time_total(self) -> None:
        self.assertAlmostEqual(parse_curl_seconds("0.004321"), 0.004321)

    def test_missing_number_is_zero(self) -> None:
        self.assertEqual(parse_curl_seconds(""), 0.0)


# ─────────────────────────────────────────────────────────────────────────────
# Effect verification — red half first
# ─────────────────────────────────────────────────────────────────────────────


class NetemVerificationTests(unittest.TestCase):
    def test_green_when_qdisc_matches(self) -> None:
        state = verify_netem_applied(
            NETEM_QDISC,
            target="node2",
            expect_delay_ms=120,
            expect_jitter_ms=30,
            expect_loss_pct=25,
        )
        self.assertEqual(state.loss_pct, 25.0)

    def test_red_when_no_qdisc_installed(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_netem_applied(
                NO_QDISC,
                target="node2",
                expect_delay_ms=120,
                expect_jitter_ms=30,
                expect_loss_pct=25,
            )
        self.assertIn("no netem qdisc present", str(caught.exception))

    def test_red_when_loss_does_not_match_request(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_netem_applied(
                NETEM_QDISC,
                target="node2",
                expect_delay_ms=120,
                expect_jitter_ms=30,
                expect_loss_pct=60,
            )
        self.assertIn("loss 25.0% != requested 60%", str(caught.exception))

    def test_absence_check_is_red_when_qdisc_survives_the_heal(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_netem_absent(NETEM_QDISC, target="node2")
        verify_netem_absent(NO_QDISC, target="node2")


class FilterVerificationTests(unittest.TestCase):
    def test_red_against_a_container_with_no_rule_installed(self) -> None:
        # The core red case: verification must fail when the fault did not land.
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_filter_present("", target="node1", ip="172.28.0.12", prio=112)
        self.assertIn("no tc filter matching 172.28.0.12 (ac1c000c)", str(caught.exception))

    def test_red_when_a_rule_exists_at_a_different_priority(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_filter_present(
                INGRESS_DROP_FILTER_HOT, target="node1", ip="172.28.0.12", prio=999
            )

    def test_red_when_a_rule_exists_for_a_different_address(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_filter_present(
                INGRESS_DROP_FILTER_HOT, target="node1", ip="172.28.0.13", prio=112
            )

    def test_green_on_the_real_dump(self) -> None:
        match = verify_filter_present(
            INGRESS_DROP_FILTER_HOT, target="node1", ip="172.28.0.12", prio=112
        )
        self.assertEqual(match.match_success, 25)

    def test_absence_check_is_red_when_the_rule_survives_the_heal(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_filter_absent(INGRESS_DROP_FILTER_HOT, target="node1", ip="172.28.0.12")
        verify_filter_absent("", target="node1", ip="172.28.0.12")


class PacketCounterTests(unittest.TestCase):
    """A rule that exists but never matches is indistinguishable from no rule."""

    def test_red_when_the_counter_did_not_move(self) -> None:
        cold = FilterMatch("ac1c000c", 12, 112, 0, 0)
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_packets_matched(cold, cold, target="node1", what="DROP rule")
        self.assertIn("not in effect", str(caught.exception))

    def test_green_when_packets_were_matched(self) -> None:
        before = verify_filter_present(
            INGRESS_DROP_FILTER_COLD, target="node1", ip="172.28.0.12", prio=112
        )
        after = verify_filter_present(
            INGRESS_DROP_FILTER_HOT, target="node1", ip="172.28.0.12", prio=112
        )
        self.assertEqual(verify_packets_matched(before, after, target="node1", what="DROP"), 25)


class ProcessVerificationTests(unittest.TestCase):
    def test_green_when_the_process_reached_state_T(self) -> None:
        stopped = verify_processes_stopped(
            parse_ps(PS_CHECKPOINTER_STOPPED), target="node3", expected_pids=[47]
        )
        self.assertEqual(stopped[0].pg_title, "checkpointer")

    def test_red_when_sigstop_did_not_take_effect(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_processes_stopped(parse_ps(PS_LEADER), target="node1", expected_pids=[25])
        self.assertIn("SIGSTOP did not take effect", str(caught.exception))

    def test_red_when_the_process_vanished_instead_of_stopping(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_processes_stopped(parse_ps(PS_LEADER), target="node1", expected_pids=[9999])
        self.assertIn("vanished instead of stopping", str(caught.exception))

    def test_scope_check_is_red_when_everything_is_stopped(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_scope_unaffected(parse_ps(PS_ALL_STOPPED), target="node3", stopped_pids=[47])
        self.assertIn("escaped its scope", str(caught.exception))

    def test_scope_check_is_green_when_the_postmaster_still_runs(self) -> None:
        verify_scope_unaffected(
            parse_ps(PS_CHECKPOINTER_STOPPED), target="node3", stopped_pids=[47]
        )


class DiskVerificationTests(unittest.TestCase):
    def test_red_when_space_remains_for_another_wal_segment(self) -> None:
        # This is precisely the pre-existing vacuous disk_full: 400 GB free
        # after a 500 MB fill.
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_out_of_space(
                parse_df(DF_UNBOUNDED), target="node1", need_bytes=WAL_SEGMENT_BYTES
            )
        self.assertIn("did not exhaust the filesystem", str(caught.exception))

    def test_green_when_less_than_one_segment_remains(self) -> None:
        verify_out_of_space(parse_df(DF_BOUNDED_FULL), target="node1", need_bytes=WAL_SEGMENT_BYTES)

    def test_unbounded_filesystem_is_refused_with_remediation(self) -> None:
        with self.assertRaises(FaultPreconditionError) as caught:
            verify_bounded_filesystem(parse_df(DF_UNBOUNDED), target="node1", max_bytes=8 * 1024**3)
        message = str(caught.exception)
        self.assertIn("PGBATTERY_STATE_SUFFIX=_bounded", message)
        self.assertIn("452.1 GiB", message)

    def test_bounded_filesystem_is_accepted(self) -> None:
        verify_bounded_filesystem(parse_df(DF_BOUNDED_EMPTY), target="node1", max_bytes=8 * 1024**3)

    def test_heal_check_is_red_while_still_full(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_space_restored(
                parse_df(DF_BOUNDED_FULL), target="node1", need_bytes=WAL_SEGMENT_BYTES
            )
        verify_space_restored(
            parse_df(DF_BOUNDED_EMPTY), target="node1", need_bytes=WAL_SEGMENT_BYTES
        )


class ClockVerificationTests(unittest.TestCase):
    def test_red_when_libfaketime_did_nothing(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_clock_offset(target="node2", observed_ms=0.0, expected_ms=2000, tolerance_ms=250)
        self.assertIn("libfaketime is not active", str(caught.exception))

    def test_red_when_the_offset_literal_was_misparsed(self) -> None:
        # `+250ms` moves the clock 250 minutes; the observed offset must not be
        # accepted as a 250 ms skew.
        with self.assertRaises(FaultEffectNotObserved):
            verify_clock_offset(
                target="node2", observed_ms=15_000_000.0, expected_ms=250, tolerance_ms=250
            )

    def test_green_within_tolerance(self) -> None:
        verify_clock_offset(target="node2", observed_ms=2043.0, expected_ms=2000, tolerance_ms=250)


class ProbeVerificationTests(unittest.TestCase):
    def test_blackhole_check_accepts_only_a_timeout(self) -> None:
        verify_probe_blackholed(target="node2", rc=28, peer="node1")
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_probe_blackholed(target="node2", rc=7, peer="node1")
        self.assertIn("packets are flowing", str(caught.exception))
        with self.assertRaises(FaultEffectNotObserved):
            verify_probe_blackholed(target="node2", rc=0, peer="node1")

    def test_reachability_check_is_red_on_any_failure(self) -> None:
        verify_probe_reachable(target="node2", rc=0, peer="node3")
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_probe_reachable(target="node2", rc=28, peer="node3")
        self.assertIn("escaped its declared scope", str(caught.exception))

    def test_added_latency_is_red_when_nothing_got_slower(self) -> None:
        with self.assertRaises(FaultEffectNotObserved) as caught:
            verify_added_latency(
                target="node2", baseline_s=0.004, observed_s=0.005, expected_delay_ms=200
            )
        self.assertIn("expected at least 100ms", str(caught.exception))

    def test_added_latency_is_green_when_the_delay_shows_up(self) -> None:
        added = verify_added_latency(
            target="node2", baseline_s=0.004, observed_s=0.210, expected_delay_ms=200
        )
        self.assertGreater(added, 100)


# ─────────────────────────────────────────────────────────────────────────────
# Whole primitives, driven by a scripted runner
# ─────────────────────────────────────────────────────────────────────────────


class PartitionLossyTests(RunnerFixture):
    def test_rejects_parameters_too_small_to_verify(self) -> None:
        self.install(ScriptedRunner([]))
        with (
            self.assertRaises(FaultPreconditionError) as caught,
            partition_lossy("node2", drop_pct=1.0, latency_ms=5),
        ):
            self.fail("body must not run")
        self.assertIn("below the verifiable floors", str(caught.exception))

    def test_red_when_the_qdisc_never_appears(self) -> None:
        # tc reports success but the qdisc is not there — the exact shape of a
        # fault that silently did not land.
        runner = self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NO_QDISC)),
                    ("curl", ok("0.004")),
                ]
            )
        )
        with (
            self.assertRaises(FaultEffectNotObserved) as caught,
            partition_lossy("node2", drop_pct=30.0, latency_ms=200),
        ):
            self.fail("body must not run")
        self.assertIn("no netem qdisc present", str(caught.exception))
        self.assertTrue(runner.matching("netem delay 200ms"))

    def test_red_when_tc_rejects_the_qdisc(self) -> None:
        self.install(
            ScriptedRunner(
                [
                    ("root netem", fail("Error: Exclusivity flag on, cannot modify.", rc=2)),
                    ("curl", ok("0.004")),
                ]
            )
        )
        with (
            self.assertRaises(Exception) as caught,
            partition_lossy("node2", drop_pct=30.0, latency_ms=200),
        ):
            self.fail("body must not run")
        self.assertIn("Exclusivity flag on", str(caught.exception))
        self.assertIn("already owns dev eth0 root", str(caught.exception))

    def test_red_when_the_delay_is_not_observable_end_to_end(self) -> None:
        self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NETEM_QDISC)),
                    ("curl", ok("0.004")),
                ]
            )
        )
        with (
            self.assertRaises(FaultEffectNotObserved) as caught,
            partition_lossy("node2", drop_pct=25.0, latency_ms=120, jitter_ms=30),
        ):
            self.fail("body must not run")
        self.assertIn("probe latency rose by only", str(caught.exception))

    def test_red_when_loss_is_configured_but_nothing_drops(self) -> None:
        probe_times = iter(["0.004", "0.200", "0.200", "0.200", "0.200", "0.200"])
        qdiscs = iter([NETEM_QDISC, NETEM_QDISC])

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "tc -s qdisc show" in cmd:
                return ok(next(qdiscs, NETEM_QDISC))
            if "curl" in cmd:
                return ok(next(probe_times, "0.200"))
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)

        with (
            self.assertRaises(FaultEffectNotObserved) as caught,
            partition_lossy("node2", drop_pct=25.0, latency_ms=120, jitter_ms=30),
        ):
            self.fail("body must not run")
        self.assertIn("netem dropped 0 packets", str(caught.exception))

    def test_green_path_verifies_then_heals(self) -> None:
        state = {"installed": False}

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "root netem" in cmd:
                state["installed"] = True
                return ok()
            if "qdisc del dev eth0 root" in cmd:
                state["installed"] = False
                return ok()
            if "tc -s qdisc show" in cmd:
                if not state["installed"]:
                    return ok(NO_QDISC)
                return ok(NETEM_QDISC.replace("dropped 0", "dropped 12"))
            if "curl" in cmd:
                return ok("0.200" if state["installed"] else "0.004")
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)

        with partition_lossy("node2", drop_pct=25.0, latency_ms=120, jitter_ms=30) as handle:
            self.assertEqual(handle.state.loss_pct, 25.0)
            self.assertEqual(handle.dropped_packets, 12)
            assert handle.added_latency_ms is not None
            self.assertGreater(handle.added_latency_ms, 60)

        self.assertFalse(state["installed"])
        kinds = [event["event"] for event in self.events]
        self.assertEqual(kinds, ["fault.open", "fault.close"])
        close = self.events[-1]
        self.assertEqual(close["primitive"], "partition_lossy")
        self.assertEqual(close["residue"], [])
        self.assertIn("held_ms", close)


class SigstopCheckpointerTests(RunnerFixture):
    def test_red_when_the_checkpointer_is_not_running(self) -> None:
        self.install(ScriptedRunner([("ps -eo", ok(PS_STANDBY.replace("checkpointer", "io w9")))]))
        with (
            self.assertRaises(FaultPreconditionError) as caught,
            sigstop_checkpointer("node3", duration_s=0.0),
        ):
            self.fail("body must not run")
        self.assertIn("not running", str(caught.exception))

    def test_red_when_sigstop_reports_success_but_state_stays_S(self) -> None:
        self.install(ScriptedRunner([("ps -eo", ok(PS_LEADER))]))
        with (
            self.assertRaises(FaultEffectNotObserved) as caught,
            sigstop_checkpointer("node1", duration_s=0.0),
        ):
            self.fail("body must not run")
        self.assertIn("SIGSTOP did not take effect", str(caught.exception))

    def test_green_path_stops_only_the_checkpointer_and_resumes_it(self) -> None:
        state = {"stopped": False}

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "kill -STOP" in cmd:
                state["stopped"] = True
                return ok()
            if "kill -CONT" in cmd:
                state["stopped"] = False
                return ok()
            if "ps -eo" in cmd:
                if state["stopped"]:
                    return ok(
                        PS_LEADER.replace(
                            "   25 Ss   postgres: checkpointer", "   25 Ts   postgres: checkpointer"
                        )
                    )
                return ok(PS_LEADER)
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)

        with sigstop_checkpointer("node1", duration_s=0.0) as handle:
            self.assertEqual(handle.pids, [25])
            self.assertEqual(handle.titles, ["checkpointer"])
        self.assertFalse(state["stopped"])
        self.assertEqual(self.events[-1]["residue"], [])


class FsyncStallTests(RunnerFixture):
    """A standby has no walwriter and a primary has no walreceiver, so the
    durability set is required as "checkpointer AND (walwriter OR walreceiver)".
    Demanding both makes the primitive refuse to run on every follower."""

    def _stateful_runner(self, ps_running: str) -> dict[str, bool]:
        state = {"stopped": False}

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "kill -STOP" in cmd:
                state["stopped"] = True
                return ok()
            if "kill -CONT" in cmd:
                state["stopped"] = False
                return ok()
            if "ps -eo" in cmd:
                if state["stopped"]:
                    return ok(re.sub(r"(\d+) Ss ", r"\1 Ts ", ps_running))
                return ok(ps_running)
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)
        return state

    def test_green_on_a_primary_stops_walwriter_not_walreceiver(self) -> None:
        state = self._stateful_runner(PS_LEADER)
        with fsync_stall("node1", duration_s=0.0) as handle:
            self.assertEqual(
                sorted(handle.titles), ["background writer", "checkpointer", "walwriter"]
            )
        self.assertFalse(state["stopped"])
        self.assertEqual(self.events[-1]["residue"], [])

    def test_green_on_a_standby_stops_walreceiver(self) -> None:
        self._stateful_runner(PS_STANDBY)
        with fsync_stall("node2", duration_s=0.0) as handle:
            titles = sorted(handle.titles)
            self.assertIn("checkpointer", titles)
            self.assertTrue(any(title.startswith("walreceiver") for title in titles))
            self.assertNotIn("walwriter", titles)

    def test_red_when_the_node_has_no_live_wal_path(self) -> None:
        # checkpointer up but neither walwriter nor walreceiver: PG is wedged
        # before recovery, so there is nothing to stall.
        wedged = (
            "   19 S    /usr/lib/postgresql/18/bin/postgres -D /d -p 5434\n"
            "   24 Ss   postgres: checkpointer\n"
        )
        self.install(ScriptedRunner([("ps -eo", ok(wedged))]))
        with self.assertRaises(FaultPreconditionError) as caught, fsync_stall("node2", 0.0):
            self.fail("body must not run")
        self.assertIn("no live WAL path to stall", str(caught.exception))

    def test_red_when_the_checkpointer_is_absent(self) -> None:
        self.install(ScriptedRunner([("ps -eo", ok(PS_STANDBY.replace("checkpointer", "io w9")))]))
        with self.assertRaises(FaultPreconditionError) as caught, fsync_stall("node2", 0.0):
            self.fail("body must not run")
        self.assertIn("checkpointer", str(caught.exception))


class DiskFullTests(RunnerFixture):
    def test_refuses_an_unbounded_volume_instead_of_filling_a_host_disk(self) -> None:
        runner = self.install(ScriptedRunner([("df -k -P", ok(DF_UNBOUNDED))]))
        with self.assertRaises(FaultPreconditionError) as caught, disk_full_during_wal("node1"):
            self.fail("body must not run")
        self.assertIn("PGBATTERY_STATE_SUFFIX=_bounded", str(caught.exception))
        self.assertEqual(runner.matching("fallocate"), [])

    def test_red_when_already_out_of_space_before_injecting(self) -> None:
        self.install(
            ScriptedRunner(
                [
                    ("df -k -P", ok(DF_BOUNDED_FULL)),
                    ("SHOW wal_segment_size", ok("16MB\n")),
                ]
            )
        )
        with self.assertRaises(FaultPreconditionError) as caught, disk_full_during_wal("node1"):
            self.fail("body must not run")
        self.assertIn("already below the", str(caught.exception))

    def test_red_when_a_wal_segment_still_allocates_after_the_fill(self) -> None:
        # The load-bearing check: df can claim the disk is nearly full while an
        # allocation still succeeds (reserved blocks, sparse accounting). Every
        # fallocate here succeeds, including the segment probe.
        state = {"filled": False}

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "SHOW wal_segment_size" in cmd:
                return ok("16MB\n")
            if "df -k -P" in cmd:
                return ok(DF_BOUNDED_FULL if state["filled"] else DF_BOUNDED_EMPTY)
            if "fallocate" in cmd:
                state["filled"] = True
                return ok()
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        with self.assertRaises(FaultEffectNotObserved) as caught, disk_full_during_wal("node1"):
            self.fail("body must not run")
        self.assertIn("still allocated successfully", str(caught.exception))

    def test_green_path_fills_verifies_and_removes_the_filler(self) -> None:
        state = {"filled": False}

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "SHOW wal_segment_size" in cmd:
                return ok("16MB\n")
            if "df -k -P" in cmd:
                return ok(DF_BOUNDED_FULL if state["filled"] else DF_BOUNDED_EMPTY)
            if "fallocate" in cmd and "segment_probe" in cmd:
                return fail("fallocate: fallocate failed: No space left on device")
            if "fallocate" in cmd:
                state["filled"] = True
                return ok()
            if "rm -f" in cmd:
                state["filled"] = False
                return ok()
            return ok()

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)

        with disk_full_during_wal("node1") as handle:
            self.assertEqual(handle.wal_segment_bytes, WAL_SEGMENT_BYTES)
            # Free space driven to below one segment, leaving half a segment.
            self.assertEqual(handle.filled_kb, 4_104_192 - 8192)
            self.assertLess(handle.usage_after.avail_bytes, WAL_SEGMENT_BYTES)
        self.assertFalse(state["filled"])
        self.assertEqual(self.events[-1]["residue"], [])


class ScrubTests(RunnerFixture):
    def test_reports_residue_it_could_not_remove(self) -> None:
        self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NETEM_QDISC)),
                    ("tc -s filter show", ok(INGRESS_DROP_FILTER_HOT)),
                    ("cat /tmp/faketime", ok("+30.000s\n")),
                    ("test -e", ok()),
                    ("ps -eo", ok(PS_CHECKPOINTER_STOPPED)),
                ]
            )
        )
        with self.assertRaises(FaultEffectNotObserved) as caught:
            scrub(["node1"])
        message = str(caught.exception)
        for expected in (
            "netem qdisc still installed",
            "ingress filters still installed",
            "faketime offset is +30.000s",
            "still present",
            "processes still stopped",
        ):
            self.assertIn(expected, message)

    def test_clean_cluster_scrubs_without_residue(self) -> None:
        self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NO_QDISC)),
                    ("tc -s filter show", ok("")),
                    ("cat /tmp/faketime", ok("+0s\n")),
                    ("test -e", fail("", rc=1)),
                    ("ps -eo", ok(PS_LEADER)),
                ]
            )
        )
        report = scrub(["node1", "node2", "node3"])
        self.assertTrue(report.clean)
        self.assertEqual(report.containers, ["node1", "node2", "node3"])

    def test_scrub_resumes_processes_left_stopped(self) -> None:
        # linearizability_register's scrub_chaos_residue does not do this, so a
        # crashed freeze leaves a stopped checkpointer for the rest of the run.
        runner = self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NO_QDISC)),
                    ("tc -s filter show", ok("")),
                    ("cat /tmp/faketime", ok("+0s\n")),
                    ("test -e", fail("", rc=1)),
                    ("ps -eo", ok(PS_CHECKPOINTER_STOPPED)),
                ]
            )
        )
        with self.assertRaises(FaultEffectNotObserved):
            scrub(["node1"])
        self.assertTrue(any("kill -CONT 47" in call for call in runner.matching("kill -CONT")))

    def test_rejects_unknown_containers(self) -> None:
        self.install(ScriptedRunner([]))
        with self.assertRaises(FaultEffectNotObserved) as caught:
            scrub(["nodeX"], verify=True)
        self.assertIn("unknown container", str(caught.exception))

    def test_scrub_clears_both_filler_paths(self) -> None:
        runner = self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NO_QDISC)),
                    ("tc -s filter show", ok("")),
                    ("cat /tmp/faketime", ok("+0s\n")),
                    ("test -e", fail("", rc=1)),
                    ("ps -eo", ok(PS_LEADER)),
                ]
            )
        )
        scrub(["node1"])
        removals = runner.matching("rm -f")
        self.assertTrue(any("_fault_fill.bin" in call for call in removals))
        self.assertTrue(any("_chaos_fill.bin" in call for call in removals))


class DataclassContractTests(unittest.TestCase):
    """The handles other harnesses read; their fields are the public surface."""

    def test_lease_boundary_skew_detects_an_early_holddown_release(self) -> None:
        # Skew of one full lease, injected 100ms into a 2000ms window: the
        # wall-clock guard sees 2100ms elapsed and releases, while the monotonic
        # lease has only actually run for 100ms.
        skew = LeaseBoundarySkew(
            container="node2",
            aim=Aim.HOLDDOWN_START,
            skew_ms=2000,
            observed_skew_ms=2000.0,
            lease_ms=2000,
            leaderless_at=100.0,
            injected_at=100.1,
        )
        self.assertAlmostEqual(skew.offset_into_window_ms, 100.0, places=3)
        self.assertTrue(skew.releases_holddown_early)

    def test_sub_lease_skew_does_not_release_the_holddown(self) -> None:
        skew = LeaseBoundarySkew(
            container="node2",
            aim=Aim.HOLDDOWN_START,
            skew_ms=1000,
            observed_skew_ms=1000.0,
            lease_ms=2000,
            leaderless_at=100.0,
            injected_at=100.1,
        )
        self.assertFalse(skew.releases_holddown_early)

    def test_direction_values_are_stable_strings(self) -> None:
        self.assertEqual(Direction.INBOUND.value, "inbound")
        self.assertEqual(Direction.OUTBOUND.value, "outbound")
        self.assertEqual(Aim.HOLDDOWN_START.value, "holddown_start")
        self.assertEqual(Aim.LEASE_EXPIRY.value, "lease_expiry")

    def test_disk_usage_byte_conversions(self) -> None:
        usage = DiskUsage("tmpfs", 4_194_304, 90_112, 4_104_192, "/var/lib/postgresql")
        self.assertEqual(usage.total_bytes, 4 * 1024**3)
        self.assertEqual(usage.avail_bytes, 4_104_192 * 1024)

    def test_netem_state_and_process_info_are_plain_values(self) -> None:
        self.assertEqual(NetemState(200.0, 50.0, 30.0).loss_pct, 30.0)
        self.assertTrue(ProcessInfo(1, "T", "postgres: checkpointer").is_stopped)


# ─────────────────────────────────────────────────────────────────────────────
# Compose object naming
# ─────────────────────────────────────────────────────────────────────────────


class ComposeProjectTests(unittest.TestCase):
    """Literal docker object names are the Class A1 bug: they resolve locally,
    where compose uses its own ``name:``, and silently do not exist under CI's
    per-run project."""

    def test_falls_back_to_the_compose_file_name(self) -> None:
        with mock.patch.dict(os.environ, clear=False):
            os.environ.pop("COMPOSE_PROJECT_NAME", None)
            self.assertEqual(compose_project(), DEFAULT_COMPOSE_PROJECT)
            self.assertEqual(cluster_network(), f"{DEFAULT_COMPOSE_PROJECT}_raft_net")

    def test_honours_a_per_run_project(self) -> None:
        with mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "pgbha_seq_42_1"}):
            self.assertEqual(compose_project(), "pgbha_seq_42_1")
            self.assertEqual(cluster_network(), "pgbha_seq_42_1_raft_net")

    def test_empty_value_falls_back_rather_than_naming_a_bare_suffix(self) -> None:
        """An empty env var would otherwise yield the network ``_raft_net``."""
        with mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": ""}):
            self.assertEqual(compose_project(), DEFAULT_COMPOSE_PROJECT)


class ParseContainerNetworksTests(unittest.TestCase):
    def test_single_network(self) -> None:
        parsed = parse_container_networks("pgbattery_raft_net=172.28.0.11 ")
        self.assertEqual(parsed, {"pgbattery_raft_net": "172.28.0.11"})

    def test_multiple_networks(self) -> None:
        parsed = parse_container_networks("a_raft_net=172.28.0.12 bridge=172.17.0.4 ")
        self.assertEqual(parsed, {"a_raft_net": "172.28.0.12", "bridge": "172.17.0.4"})

    def test_attached_without_an_address_is_not_absent(self) -> None:
        self.assertEqual(parse_container_networks("net="), {"net": ""})

    def test_detached_container_reports_nothing(self) -> None:
        self.assertEqual(parse_container_networks("   "), {})


class NetworkVerificationTests(unittest.TestCase):
    NET = "proj_raft_net"

    def test_detach_unobserved_when_still_attached(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_detached({self.NET: "172.28.0.11"}, target="node1", network=self.NET)

    def test_detach_observed_when_gone(self) -> None:
        verify_detached({"bridge": "172.17.0.2"}, target="node1", network=self.NET)

    def test_reattach_unobserved_when_absent(self) -> None:
        with self.assertRaises(FaultEffectNotObserved):
            verify_attached({}, target="node1", network=self.NET)

    def test_reattach_unobserved_when_off_subnet(self) -> None:
        """A reattach that lands outside 172.28.x is not the topology the rest of
        the suite assumes."""
        with self.assertRaises(FaultEffectNotObserved):
            verify_attached({self.NET: "10.0.0.5"}, target="node1", network=self.NET)

    def test_reattach_observed_on_cluster_subnet(self) -> None:
        verify_attached({self.NET: "172.28.0.11"}, target="node1", network=self.NET)


class NetworkRunner:
    """Answers a `network_detached` window, sequencing the inspect calls.

    `attached_sequence` is the answer to each successive ``docker inspect``:
    attached before the detach, absent after it, attached again after the heal.
    """

    def __init__(
        self,
        network: str,
        *,
        attached_sequence: Sequence[bool],
        ip: str = "172.28.0.11",
        cid: str = "c0ffee123456",
        detach_result: CommandResult | None = None,
        reattach_result: CommandResult | None = None,
    ) -> None:
        self.network = network
        self.attached_sequence = list(attached_sequence)
        self.ip = ip
        self.cid = cid
        self.detach_result = detach_result
        self.reattach_result = reattach_result
        self.calls: list[str] = []
        self.inspects = 0

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        self.calls.append(cmd)
        if "docker compose ps -q" in cmd:
            return CommandResult(0, f"{self.cid}\n", "")
        if "docker inspect" in cmd:
            idx = min(self.inspects, len(self.attached_sequence) - 1)
            self.inspects += 1
            attached = self.attached_sequence[idx]
            return CommandResult(0, f"{self.network}={self.ip} " if attached else "", "")
        if "docker network disconnect" in cmd and self.detach_result is not None:
            return self.detach_result
        if "docker network connect" in cmd and self.reattach_result is not None:
            return self.reattach_result
        return CommandResult(0, "", "")

    def matching(self, needle: str) -> list[str]:
        return [c for c in self.calls if needle in c]


class NetworkDetachedTests(unittest.TestCase):
    PROJECT = "pgbha_elle_99_1"

    def setUp(self) -> None:
        self.network = f"{self.PROJECT}_raft_net"
        patcher = mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": self.PROJECT})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.events: list[dict[str, object]] = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)

    def install(self, runner: NetworkRunner) -> NetworkRunner:
        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        return runner

    def test_detach_names_the_active_projects_network(self) -> None:
        """The regression guard: under a per-run project the disconnect must name
        that project's network, never the compose file's default."""
        runner = self.install(NetworkRunner(self.network, attached_sequence=[True, False, True]))
        with network_detached("node1"):
            pass
        disconnects = runner.matching("docker network disconnect")
        self.assertEqual(len(disconnects), 1, runner.calls)
        self.assertIn(self.network, disconnects[0])
        self.assertIn("c0ffee123456", disconnects[0])
        self.assertNotIn(f"{DEFAULT_COMPOSE_PROJECT}_raft_net", disconnects[0])

    def test_reattaches_with_the_address_it_read(self) -> None:
        """Reading the address beats deriving it from a node index, which cannot
        notice that it is wrong."""
        runner = self.install(
            NetworkRunner(self.network, attached_sequence=[True, False, True], ip="172.28.0.13")
        )
        with network_detached("node3") as handle:
            self.assertEqual(handle.restore_ip, "172.28.0.13")
            self.assertEqual(handle.network, self.network)
        connects = runner.matching("docker network connect")
        self.assertEqual(len(connects), 1, runner.calls)
        self.assertIn("--ip 172.28.0.13", connects[0])

    def test_raises_when_the_node_was_never_attached(self) -> None:
        runner = self.install(NetworkRunner(self.network, attached_sequence=[False]))
        with self.assertRaises(FaultPreconditionError), network_detached("node1"):
            pass
        self.assertEqual(runner.matching("docker network disconnect"), [])

    def test_raises_when_the_detach_does_not_take_effect(self) -> None:
        """The Class A1 shape: the command reports success and nothing happened."""
        runner = self.install(NetworkRunner(self.network, attached_sequence=[True, True]))
        with self.assertRaises(FaultEffectNotObserved), network_detached("node1"):
            pass
        self.assertEqual(len(runner.matching("docker network disconnect")), 1)

    def test_raises_when_the_detach_command_fails(self) -> None:
        self.install(
            NetworkRunner(
                self.network,
                attached_sequence=[True, False, True],
                detach_result=CommandResult(1, "", "No such network"),
            )
        )
        with self.assertRaises(FaultInjectionError), network_detached("node1"):
            pass

    def test_raises_when_the_reattach_fails(self) -> None:
        """A node left off the network poisons every later case, so this must be
        loud rather than best-effort."""
        self.install(
            NetworkRunner(
                self.network,
                attached_sequence=[True, False, False],
                reattach_result=CommandResult(1, "", "address already in use"),
            )
        )
        with self.assertRaises(FaultInjectionError), network_detached("node1"):
            pass

    def test_raises_when_the_reattach_is_not_observed(self) -> None:
        self.install(NetworkRunner(self.network, attached_sequence=[True, False, False]))
        with self.assertRaises(FaultEffectNotObserved), network_detached("node1"):
            pass

    def test_emits_inject_and_heal_events(self) -> None:
        self.install(NetworkRunner(self.network, attached_sequence=[True, False, True]))
        with network_detached("node1"):
            pass
        kinds = [(e.get("event"), e.get("primitive")) for e in self.events]
        self.assertIn(("inject", "network_detached"), kinds)
        self.assertIn(("heal", "network_detached"), kinds)

    def test_unresolvable_service_is_a_precondition_failure(self) -> None:
        """No container id means the project is wrong or the cluster is down —
        either way nothing can be injected."""

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "docker compose ps -q" in cmd:
                return CommandResult(0, "\n", "")
            return CommandResult(0, "", "")

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        with self.assertRaises(FaultPreconditionError):
            container_id("node1")


IPTABLES_PORT_DROP = """\
-P INPUT ACCEPT
-A INPUT -s 172.28.0.12/32 -p tcp -m tcp --dport 5434 -j DROP
-A INPUT -s 172.28.0.12/32 -p tcp -m tcp --sport 5434 -j DROP
--- counters ---
Chain INPUT (policy ACCEPT 0 packets, 0 bytes)
 pkts bytes target prot opt in out source        destination
   17  1020 DROP   tcp  --  *  *   172.28.0.12   0.0.0.0/0    tcp dpt:5434
    0     0 DROP   tcp  --  *  *   172.28.0.12   0.0.0.0/0    tcp spt:5434
"""

IPTABLES_PORT_DROP_COLD = IPTABLES_PORT_DROP.replace("   17  1020 DROP", "    0     0 DROP")

IPTABLES_EMPTY_CHAIN = """\
-P INPUT ACCEPT
--- counters ---
Chain INPUT (policy ACCEPT 0 packets, 0 bytes)
 pkts bytes target prot opt in out source destination
"""


class ChannelTests(unittest.TestCase):
    def test_each_channel_maps_to_its_port(self) -> None:
        self.assertEqual(Channel.RAFT.port, 5433)
        self.assertEqual(Channel.REPLICATION.port, 5434)
        self.assertEqual(Channel.GATEWAY.port, 5432)
        self.assertEqual(Channel.MANAGEMENT.port, 9091)

    def test_channels_are_distinct_ports(self) -> None:
        """Two channels sharing a port would make a gray split unexpressible."""
        ports = [c.port for c in Channel]
        self.assertEqual(len(ports), len(set(ports)))


class PortDropParsingTests(unittest.TestCase):
    def test_rule_present_is_detected(self) -> None:
        self.assertTrue(
            parse_port_drop_rule(IPTABLES_PORT_DROP, "172.28.0.12", 5434, from_listener=False)
        )

    def test_rule_for_another_port_does_not_match(self) -> None:
        """The whole point is severing one protocol; matching the wrong port
        would report a replication cut that never happened."""
        self.assertFalse(
            parse_port_drop_rule(IPTABLES_PORT_DROP, "172.28.0.12", 5433, from_listener=False)
        )

    def test_rule_for_another_peer_does_not_match(self) -> None:
        self.assertFalse(
            parse_port_drop_rule(IPTABLES_PORT_DROP, "172.28.0.13", 5434, from_listener=False)
        )

    def test_empty_chain_has_no_rule(self) -> None:
        self.assertFalse(
            parse_port_drop_rule(IPTABLES_EMPTY_CHAIN, "172.28.0.12", 5434, from_listener=False)
        )

    def test_packet_counter_is_read(self) -> None:
        self.assertEqual(parse_port_drop_packets(IPTABLES_PORT_DROP, "172.28.0.12", 5434), 17)

    def test_cold_rule_reports_zero(self) -> None:
        self.assertEqual(parse_port_drop_packets(IPTABLES_PORT_DROP_COLD, "172.28.0.12", 5434), 0)

    def test_counter_for_another_port_is_zero(self) -> None:
        self.assertEqual(parse_port_drop_packets(IPTABLES_PORT_DROP, "172.28.0.12", 5433), 0)


class PortDropRunner:
    """Renders iptables output from the rules actually installed via -I / -D.

    A static transcript cannot express "installed, then removed", which is
    exactly what the heal verification checks.
    """

    def __init__(self, *, packets: int = 17) -> None:
        self.rules: set[tuple[str, int, bool]] = set()
        self.packets = packets
        self.calls: list[str] = []
        self.insert_result: CommandResult | None = None
        self.refuse_delete = False

    @staticmethod
    def _parse(cmd: str) -> tuple[str, int, bool]:
        match = re.search(r"-s (\S+) --(s|d)port (\d+)", cmd)
        assert match is not None, cmd
        return match.group(1), int(match.group(3)), match.group(2) == "s"

    def _render(self) -> str:
        lines = ["-P INPUT ACCEPT"]
        lines += [
            f"-A INPUT -s {ip}/32 -p tcp -m tcp "
            f"--{'sport' if from_listener else 'dport'} {port} -j DROP"
            for ip, port, from_listener in sorted(self.rules)
        ]
        lines += ["--- counters ---", "Chain INPUT (policy ACCEPT 0 packets, 0 bytes)"]
        lines += [" pkts bytes target prot opt in out source destination"]
        lines += [
            f"   {self.packets}  1020 DROP tcp -- * * {ip} 0.0.0.0/0 tcp "
            f"{'spt' if from_listener else 'dpt'}:{port}"
            for ip, port, from_listener in sorted(self.rules)
        ]
        return "\n".join(lines) + "\n"

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        self.calls.append(cmd)
        if "iptables -I INPUT" in cmd:
            if self.insert_result is not None:
                return self.insert_result
            self.rules.add(self._parse(cmd))
        elif "iptables -D INPUT" in cmd:
            if self.refuse_delete:
                return CommandResult(1, "", "iptables: Bad rule")
            self.rules.discard(self._parse(cmd))
        elif "iptables -S" in cmd:
            return CommandResult(0, self._render(), "")
        return CommandResult(0, "", "")

    def matching(self, needle: str) -> list[str]:
        return [c for c in self.calls if needle in c]


class PartitionChannelTests(unittest.TestCase):
    def install(self, runner: PortDropRunner) -> PortDropRunner:
        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.events: list[dict[str, object]] = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)
        return runner

    def test_severs_only_the_named_channel(self) -> None:
        runner = self.install(PortDropRunner())
        with partition_channel("node1", ["node2"], Channel.REPLICATION, settle_s=0.0) as handle:
            self.assertEqual(handle.port, 5434)
            # Two rules per peer (dport + sport), so the counter sums both.
            self.assertEqual(handle.dropped_packets, 34)
        inserts = runner.matching("iptables -I INPUT")
        self.assertEqual(len(inserts), 2)
        self.assertTrue(any("--dport 5434" in c for c in inserts))
        self.assertTrue(any("--sport 5434" in c for c in inserts))
        # Raft must be left alone, or this is just a full partition.
        self.assertFalse(any("5433" in c for c in inserts))

    def test_heals_every_rule_it_installed(self) -> None:
        runner = self.install(PortDropRunner())
        with partition_channel("node1", ["node2", "node3"], Channel.RAFT, settle_s=0.0):
            self.assertEqual(len(runner.rules), 4)
        self.assertEqual(len(runner.matching("iptables -D INPUT")), 4)
        self.assertEqual(runner.rules, set())

    def test_rule_that_matched_nothing_is_a_failure(self) -> None:
        """The Class A1 shape: the rule is installed, and partitions nothing."""
        self.install(PortDropRunner(packets=0))
        with (
            self.assertRaises(FaultEffectNotObserved),
            partition_channel("node1", ["node2"], Channel.REPLICATION, settle_s=0.0),
        ):
            pass

    def test_failed_insert_raises(self) -> None:
        runner = self.install(PortDropRunner())
        runner.insert_result = CommandResult(1, "", "iptables: Permission denied")
        with (
            self.assertRaises(FaultInjectionError),
            partition_channel("node1", ["node2"], Channel.REPLICATION, settle_s=0.0),
        ):
            pass

    def test_rule_surviving_the_heal_raises(self) -> None:
        """A DROP left behind poisons every later case in the run."""
        runner = self.install(PortDropRunner())
        with (
            self.assertRaises(FaultEffectNotObserved),
            partition_channel("node1", ["node2"], Channel.REPLICATION, settle_s=0.0),
        ):
            runner.refuse_delete = True

    def test_require_traffic_can_be_waived(self) -> None:
        """Only for channels with no steady traffic to wait for."""
        self.install(PortDropRunner(packets=0))
        with partition_channel(
            "node1", ["node2"], Channel.GATEWAY, require_traffic=False, settle_s=0.0
        ) as handle:
            self.assertEqual(handle.dropped_packets, 0)

    def test_emits_inject_and_heal_events(self) -> None:
        self.install(PortDropRunner())
        with partition_channel("node1", ["node2"], Channel.RAFT, settle_s=0.0):
            pass
        kinds = [(e.get("event"), e.get("primitive")) for e in self.events]
        self.assertIn(("inject", "partition_channel"), kinds)
        self.assertIn(("heal", "partition_channel"), kinds)


class ContainerNetworksCmdTests(unittest.TestCase):
    def test_command_targets_the_given_container(self) -> None:
        cmd = container_networks_cmd("abc123")
        self.assertIn("docker inspect", cmd)
        self.assertTrue(cmd.endswith("abc123"))

    def test_command_asks_for_name_and_address(self) -> None:
        cmd = container_networks_cmd("abc123")
        self.assertIn(".NetworkSettings.Networks", cmd)
        self.assertIn(".IPAddress", cmd)


class ContainerRunStateTests(unittest.TestCase):
    """`started_at` is what separates an observed restart from an assumed one."""

    RUNNING = "running 2026-07-30T12:00:00.000000000Z 0\n"
    RESTARTED = "running 2026-07-30T12:05:00.000000000Z 1\n"

    def test_parses_the_three_fields(self) -> None:
        state = fp.parse_container_runstate(self.RUNNING)
        assert state is not None
        self.assertEqual(state.status, "running")
        self.assertEqual(state.restart_count, 0)

    def test_unreadable_output_is_none_not_a_default(self) -> None:
        """A docker error must not decode as a stopped container."""
        for text in ("", "Error: No such object: node1", "running", "a b c"):
            with self.subTest(text=text):
                self.assertIsNone(fp.parse_container_runstate(text))

    def test_incarnation_check_rejects_an_unchanged_container(self) -> None:
        """`docker kill` on an already-dead container exits 0 and changes
        nothing; that no-op is what this refuses to accept."""
        before = fp.parse_container_runstate(self.RUNNING)
        with self.assertRaises(fp.FaultEffectNotObserved):
            fp.verify_incarnation_changed(
                before, before, target="node1", action="restart_container"
            )

    def test_incarnation_check_accepts_a_new_incarnation(self) -> None:
        before = fp.parse_container_runstate(self.RUNNING)
        after = fp.parse_container_runstate(self.RESTARTED)
        fp.verify_incarnation_changed(before, after, target="node1", action="restart_container")

    def test_incarnation_check_rejects_an_unreadable_result(self) -> None:
        before = fp.parse_container_runstate(self.RUNNING)
        with self.assertRaises(fp.FaultEffectNotObserved):
            fp.verify_incarnation_changed(before, None, target="node1", action="kill_container")

    def test_status_check_reports_the_mismatch(self) -> None:
        state = fp.parse_container_runstate(self.RUNNING)
        with self.assertRaises(fp.FaultEffectNotObserved) as caught:
            fp.verify_status(state, target="node1", expected="paused")
        self.assertIn("paused", str(caught.exception))
        fp.verify_status(state, target="node1", expected="running")

    def test_runstate_command_asks_for_what_the_dataclass_carries(self) -> None:
        cmd = fp.container_runstate_cmd("abc123")
        for field in (".State.Status", ".State.StartedAt", ".RestartCount"):
            self.assertIn(field, cmd)
        self.assertTrue(cmd.endswith("abc123"))


if __name__ == "__main__":
    unittest.main()
