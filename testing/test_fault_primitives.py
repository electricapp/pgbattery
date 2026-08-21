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
import threading
import tomllib
import unittest
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from unittest import mock

import fault_primitives as fp
import harness_fakes as hf
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
    root_qdisc_lock,
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
from harness_fakes import ScriptedRunner, fail, ok

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


# The doubles live in `harness_fakes` because the Raft torn-write tests need
# the same ones, and a fake copied is a fake that drifts.
RunnerFixture = hf.HarnessFixture


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

    def test_every_duration_constructor_is_understood(self) -> None:
        # A constant rewritten from `from_secs(60)` to `from_mins(1)` is the
        # same duration. Matching only secs and millis read that as an absent
        # constant and refused the run.
        for unit, expected in (
            ("nanos", 0),
            ("micros", 1),
            ("millis", 1_000),
            ("secs", 1_000_000),
            ("mins", 60_000_000),
            ("hours", 3_600_000_000),
        ):
            source = f"pub const T: Duration = Duration::from_{unit}(1_000);"
            self.assertEqual(parse_rust_duration_const_ms(source, "T"), expected, unit)

    def test_a_constructor_std_does_not_have_is_not_invented(self) -> None:
        with self.assertRaises(FaultPreconditionError):
            parse_rust_duration_const_ms("pub const T: Duration = Duration::from_years(1);", "T")


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

    def test_the_same_device_hands_back_the_same_lock(self) -> None:
        """One root qdisc per device means one lock per device, not per call."""
        self.assertIs(root_qdisc_lock("node2"), root_qdisc_lock("node2"))

    def test_a_different_node_is_a_different_device(self) -> None:
        """Serialising every node against every other would throw away most of
        what a storm is for — faults on separate nodes really do overlap."""
        self.assertIsNot(root_qdisc_lock("node2"), root_qdisc_lock("node3"))

    def test_partition_lossy_waits_for_the_device_instead_of_colliding(self) -> None:
        """The chaos_storm regression: two netem faults drawn onto one node.

        tc permits one root qdisc per device, so the second `add` used to fail
        with "Exclusivity flag on" and end the run — a collision between two
        faults reported as though the cluster had done something wrong. Holding
        the device here stands in for the first fault's window: the second must
        issue no command at all until the device is free.
        """
        runner = self.install(
            ScriptedRunner(
                [
                    ("tc -s qdisc show", ok(NETEM_QDISC)),
                    ("curl", ok("0.4")),
                ]
            )
        )
        device = root_qdisc_lock("node2")
        finished = threading.Event()
        self.assertTrue(device.acquire(timeout=1.0), "device lock was already held")

        def second_fault() -> None:
            # This scripting cannot produce an observable latency rise, so the
            # window raises. Irrelevant here: what is under test is whether tc
            # was touched before the device was free.
            with (
                suppress(FaultEffectNotObserved),
                partition_lossy("node2", drop_pct=0.0, latency_ms=200),
            ):
                pass
            finished.set()

        worker = threading.Thread(target=second_fault, daemon=True)
        worker.start()
        try:
            self.assertFalse(
                finished.wait(timeout=0.5),
                "the second fault ran to completion while the device was held",
            )
            self.assertEqual(runner.calls, [], "the second fault ran tc before it owned the device")
        finally:
            device.release()
        worker.join(timeout=5.0)
        self.assertTrue(finished.is_set(), "the second fault never ran after the device freed")
        self.assertTrue(
            runner.matching("netem delay 200ms"),
            "the second fault never installed its qdisc once the device was free",
        )

    def test_a_device_that_never_frees_fails_rather_than_hanging(self) -> None:
        """The lock is not reentrant, so a nested or leaked window would block
        forever. tc used to reject that case immediately; the bound keeps it a
        failure rather than turning it into a hung run."""
        self.install(ScriptedRunner([]))
        device = root_qdisc_lock("node2")
        self.assertTrue(device.acquire(timeout=1.0), "device lock was already held")
        self.addCleanup(device.release)
        with (
            mock.patch.object(fp, "ROOT_QDISC_WAIT_S", 0.05),
            self.assertRaises(FaultPreconditionError) as caught,
            partition_lossy("node2", drop_pct=0.0, latency_ms=200),
        ):
            self.fail("body must not run")
        self.assertIn("root qdisc was still held", str(caught.exception))

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
        if "docker compose ps -aq" in cmd:
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

    def test_a_stopped_container_still_resolves(self) -> None:
        """`start_container` has to name a container that is not running.

        Compose reports nothing for a stopped container without `-a`, so a
        resolution that omitted it could never heal a `kill_container`: the
        service is stopped by definition at that point. This mock answers only
        the `-aq` form, which is exactly the asymmetry the real docker CLI has.
        """

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "docker compose ps -aq" in cmd:
                return CommandResult(0, "deadbeefcafe\n", "")
            if "docker compose ps -q" in cmd:
                return CommandResult(0, "\n", "")
            return CommandResult(0, "", "")

        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        self.assertEqual(container_id("node1"), "deadbeefcafe")

    def test_unresolvable_service_is_a_precondition_failure(self) -> None:
        """No container id means the project is wrong or the cluster is down —
        either way nothing can be injected."""

        def runner(cmd: str, timeout_s: float) -> CommandResult:
            if "docker compose ps -aq" in cmd:
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


class LazyfsFaultChannelTests(RunnerFixture):
    """The FIFO liveness probe, which exists because a silent LazyFS is the
    worst failure this suite has: writes to the control FIFO succeed whether or
    not anything is reading it, so `clear-cache` reported success and discarded
    nothing for as long as `fifo_path_completed` was set."""

    WORKER_LOG = (
        "[2026-01-01 00:00:00.000] [console] [info] [lazyfs.faults.worker]: "
        "waiting for fault commands...\n"
    )

    def test_probe_command_is_one_lazyfs_cannot_implement(self) -> None:
        """If the probe were ever a real command it would have side effects,
        and the liveness check would be injecting a fault to test for one."""
        probe = fp.lazyfs_probe_command("deadbeef")
        self.assertTrue(probe.startswith("lazyfs::"))
        for real in ("clear-cache", "cache-checkpoint", "crash", "torn-op", "torn-seq", "help"):
            self.assertNotEqual(probe, f"lazyfs::{real}")

    def test_probe_command_rejects_a_nonce_that_could_break_the_grammar(self) -> None:
        """LazyFS splits commands on `::` and `=`; a nonce carrying either
        would be parsed as another attribute rather than echoed intact."""
        for bad in ("", "a::b", "a=b", "with space"):
            with self.subTest(nonce=bad), self.assertRaises(ValueError):
                fp.lazyfs_probe_command(bad)

    def test_consumed_requires_the_workers_own_echo(self) -> None:
        """The nonce appearing somewhere in the log is not evidence. Only the
        worker's `command unknown` line means the command was read."""
        nonce = "abc123"
        probe = fp.lazyfs_probe_command(nonce)
        self.assertFalse(fp.parse_lazyfs_consumed("", nonce))
        self.assertFalse(fp.parse_lazyfs_consumed(f"wrote {probe} to the fifo\n", nonce))
        self.assertFalse(fp.parse_lazyfs_consumed(fp.lazyfs_probe_command("other"), nonce))
        self.assertTrue(
            fp.parse_lazyfs_consumed(f"[lazyfs.faults.worker]: command unknown '{probe}'\n", nonce)
        )

    def test_received_count_distinguishes_this_command_from_the_last(self) -> None:
        """Presence would match a previous injection's line and pass without
        the current command having been read at all."""
        line = "[lazyfs.faults.worker]: received 'lazyfs::clear-cache'\n"
        self.assertEqual(fp.count_lazyfs_received("", "lazyfs::clear-cache"), 0)
        self.assertEqual(fp.count_lazyfs_received(line, "lazyfs::clear-cache"), 1)
        self.assertEqual(fp.count_lazyfs_received(line * 3, "lazyfs::clear-cache"), 3)

    def test_received_count_refuses_a_non_control_word(self) -> None:
        with self.assertRaises(ValueError):
            fp.count_lazyfs_received("", "clear-cache")

    def test_log_read_tolerates_a_missing_log(self) -> None:
        """LazyFS truncates the log at startup, so the caller polls through a
        window where it does not exist. That must not read as a failure."""
        self.assertIn("2>/dev/null", fp.lazyfs_log_cmd())
        self.assertIn("|| true", fp.lazyfs_log_cmd())

    def test_channel_check_fails_when_the_worker_never_echoes(self) -> None:
        """The red half: a parked worker still accepts FIFO writes. This is
        precisely the state a set `fifo_path_completed` produces."""
        runner = self.install(
            ScriptedRunner(
                [
                    ("lazyfs.fifo", ok()),
                    ("lazyfs.log", ok(self.WORKER_LOG)),
                ]
            )
        )
        with self.assertRaises(fp.FaultPreconditionError) as caught:
            fp.verify_lazyfs_fault_channel("node1", timeout_s=0.5)
        self.assertIn("fifo_path_completed", str(caught.exception))
        self.assertTrue(runner.matching("lazyfs.fifo"))

    def test_channel_check_passes_when_the_worker_echoes_the_probe(self) -> None:
        """Green half. The echo has to carry the nonce this call generated,
        so the fixture derives it from the command the probe actually sent."""
        sent: list[str] = []

        def scripted(cmd: str, timeout_s: float) -> fp.CommandResult:
            sent.append(cmd)
            if "lazyfs.log" in cmd:
                echoed = [
                    f"[lazyfs.faults.worker]: command unknown '{probe}'"
                    for probe in (_probe_in(text) for text in sent)
                    if probe
                ]
                return ok(self.WORKER_LOG + "\n".join(echoed))
            return ok()

        def _probe_in(text: str) -> str | None:
            match = re.search(r"lazyfs::pgbattery-probe-[0-9a-f]+", text)
            return match.group(0) if match else None

        previous = fp.set_command_runner(scripted)
        self.addCleanup(fp.set_command_runner, previous)
        fp.verify_lazyfs_fault_channel("node1", timeout_s=5.0)

    def test_channel_check_fails_when_the_fifo_write_itself_fails(self) -> None:
        self.install(ScriptedRunner([("lazyfs.fifo", fail("No such file or directory"))]))
        with self.assertRaises(fp.FaultPreconditionError):
            fp.verify_lazyfs_fault_channel("node1", timeout_s=0.5)

    def test_channel_check_waits_out_an_undeliverable_exec(self) -> None:
        """An exec that never reached the container says nothing about the fault
        worker, so the gate must retry rather than give up on it. Both shapes
        that failed torn-raft in CI: runc unable to enter the namespaces, and a
        non-zero exit with nothing on either stream."""
        sent: list[str] = []
        attempts = {"fifo": 0}
        undeliverable = [fail(SETNS_FAILURE), CommandResult(1, "", "")]

        def scripted(cmd: str, timeout_s: float) -> fp.CommandResult:
            if "lazyfs.fifo" in cmd:
                attempts["fifo"] += 1
                if attempts["fifo"] <= len(undeliverable):
                    return undeliverable[attempts["fifo"] - 1]
                sent.append(cmd)
                return ok()
            if "lazyfs.log" in cmd:
                echoed = [
                    f"[lazyfs.faults.worker]: command unknown '{probe}'"
                    for probe in (_probe_in(text) for text in sent)
                    if probe
                ]
                return ok(self.WORKER_LOG + "\n".join(echoed))
            return ok()

        def _probe_in(text: str) -> str | None:
            match = re.search(r"lazyfs::pgbattery-probe-[0-9a-f]+", text)
            return match.group(0) if match else None

        previous = fp.set_command_runner(scripted)
        self.addCleanup(fp.set_command_runner, previous)
        fp.verify_lazyfs_fault_channel("node1", timeout_s=10.0)
        self.assertEqual(attempts["fifo"], len(undeliverable) + 1)


class TornWriteTests(RunnerFixture):
    """Torn-write injection (H-25). Every guard here encodes something read out
    of the LazyFS source, because each one fails silently rather than loudly:
    a torn-op that does not match reports "configured successfully" and tears
    nothing."""

    PAGE = f"{fp.LAZYFS_ROOT_DIR}/base/5/16384"

    def test_command_uses_the_root_path(self) -> None:
        cmd = fp.lazyfs_torn_op_cmd(self.PAGE, parts=2, persist=(1,))
        self.assertEqual(cmd, f"lazyfs::torn-op::file={self.PAGE}::persist=1::parts=2")

    def test_mount_path_is_refused(self) -> None:
        """LazyFS keys its fault table by the path its callbacks receive, which
        the subdir module has already rewritten to the backing root. A mount
        path matches nothing and the fault never fires."""
        with self.assertRaises(ValueError) as caught:
            fp.lazyfs_torn_op_cmd(f"{fp.PG_DATA_DIR}/base/5/16384", parts=2, persist=(1,))
        self.assertIn("LazyFS root", str(caught.exception))

    def test_a_path_in_the_other_instance_is_refused(self) -> None:
        """The two instances have separate fault tables. Arming a path from one
        against the other's FIFO configures a fault that can never match, and
        LazyFS reports that as success."""
        raft_page = f"{fp.LAZYFS_RAFT.root_dir}/{fp.RAFT_DB_FILE}"
        with self.assertRaises(ValueError) as caught:
            fp.lazyfs_torn_op_cmd(raft_page, parts=2, persist=(1,), mount=fp.LAZYFS_DATA)
        self.assertIn("pgdata", str(caught.exception))
        # ...and the same path against its own instance is fine.
        fp.lazyfs_torn_op_cmd(raft_page, parts=2, persist=(1,), mount=fp.LAZYFS_RAFT)

    def test_the_two_instances_share_nothing(self) -> None:
        """A shared FIFO or log would send faults to whichever instance won the
        race, and no assertion downstream could tell that from a fault that did
        nothing."""
        data, raft = fp.LAZYFS_DATA, fp.LAZYFS_RAFT
        self.assertNotEqual(data.fifo, raft.fifo)
        self.assertNotEqual(data.log, raft.log)
        self.assertNotEqual(data.root_dir, raft.root_dir)
        self.assertNotEqual(data.mount_dir, raft.mount_dir)

    def test_holds_requires_a_path_below_the_root(self) -> None:
        """A prefix match on the bare root would accept a sibling directory
        whose name merely starts the same way."""
        raft = fp.LAZYFS_RAFT
        self.assertTrue(raft.holds(f"{raft.root_dir}/{fp.RAFT_DB_FILE}"))
        self.assertFalse(raft.holds(raft.root_dir))
        self.assertFalse(raft.holds(f"{raft.root_dir}-backup/raft.db"))

    def test_persisting_every_part_is_refused(self) -> None:
        """Not a torn write at all: LazyFS would write the whole thing and log
        success, which is the shape of a fault that cannot fail."""
        with self.assertRaises(ValueError):
            fp.lazyfs_torn_op_cmd(self.PAGE, parts=2, persist=(1, 2))

    def test_degenerate_parameters_are_refused(self) -> None:
        for parts, persist in ((1, (1,)), (2, ()), (2, (0,)), (2, (3,)), (3, (1, 1))):
            with self.subTest(parts=parts, persist=persist), self.assertRaises(ValueError):
                fp.lazyfs_torn_op_cmd(self.PAGE, parts=parts, persist=persist)

    def test_torn_bytes_sums_the_surviving_pieces(self) -> None:
        log = f"[lazyfs.faults]: Write to path {self.PAGE}: will persist 4096 bytes from offset 0\n"
        self.assertEqual(fp.parse_lazyfs_torn_bytes(log, self.PAGE), 4096)
        self.assertEqual(fp.parse_lazyfs_torn_bytes(log * 2, self.PAGE), 8192)

    def test_torn_bytes_is_none_when_the_fault_never_fired(self) -> None:
        """Configured-but-never-fired is the dangerous state: the file is whole
        and every downstream assertion passes vacuously."""
        armed = "[lazyfs.faults.worker]: configured successfully 'lazyfs::torn-op::...'\n"
        self.assertIsNone(fp.parse_lazyfs_torn_bytes(armed, self.PAGE))
        self.assertIsNone(fp.parse_lazyfs_torn_bytes("", self.PAGE))

    def test_torn_bytes_does_not_match_another_file(self) -> None:
        other = f"{fp.LAZYFS_ROOT_DIR}/base/5/99999"
        log = f"[lazyfs.faults]: Write to path {other}: will persist 4096 bytes from offset 0\n"
        self.assertIsNone(fp.parse_lazyfs_torn_bytes(log, self.PAGE))

    def test_injection_check_fails_when_nothing_was_torn(self) -> None:
        """The red half."""
        self.install(ScriptedRunner([("lazyfs.log", ok("nothing happened here\n"))]))
        with self.assertRaises(fp.FaultEffectNotObserved) as caught:
            fp.verify_torn_write_injected("node1", self.PAGE)
        self.assertIn("vacuous", str(caught.exception))

    def test_injection_check_reports_the_surviving_bytes(self) -> None:
        log = f"[lazyfs.faults]: Write to path {self.PAGE}: will persist 4096 bytes from offset 0\n"
        self.install(ScriptedRunner([("lazyfs.log", ok(log))]))
        self.assertEqual(fp.verify_torn_write_injected("node1", self.PAGE), 4096)

    def test_injection_check_rejects_a_different_split(self) -> None:
        """A tear of the wrong size means the write that landed was not the
        write the test meant to tear."""
        log = f"[lazyfs.faults]: Write to path {self.PAGE}: will persist 512 bytes from offset 0\n"
        self.install(ScriptedRunner([("lazyfs.log", ok(log))]))
        with self.assertRaises(fp.FaultEffectNotObserved):
            fp.verify_torn_write_injected("node1", self.PAGE, expected_bytes=4096)


class LazyfsCheckpointTests(unittest.TestCase):
    """`flush_lazyfs_cache` must wait for the checkpoint to be applied.

    Writing to the control FIFO succeeds whether or not anything reads it, so
    a flush that trusted the write would report a persisted cache to a caller
    about to destroy the container.
    """

    def flush(self, log: str, sent_rc: int = 0) -> None:
        replies = {
            "log": fp.CommandResult(0, log, ""),
            "send": fp.CommandResult(sent_rc, "", ""),
        }

        def exec_in(container: str, cmd: str, **_: object) -> fp.CommandResult:
            return replies["send"] if "fifo" in cmd else replies["log"]

        with (
            mock.patch.object(fp, "exec_in", side_effect=exec_in),
            mock.patch("time.sleep"),
        ):
            fp.flush_lazyfs_cache("node2", mount=fp.LAZYFS_DATA, timeout_s=0.05)

    def test_an_applied_checkpoint_is_accepted(self) -> None:
        # The count has to rise, so the log must already carry one fewer than
        # it will after the flush. One line before, one after is the same
        # count; two is a rise.
        replies = iter(
            [
                fp.CommandResult(0, "", ""),
                fp.CommandResult(0, "", ""),
                fp.CommandResult(0, f"[lazyfs.cmds]: {fp.LAZYFS_CHECKPOINT_DONE}\n", ""),
            ]
        )
        with (
            mock.patch.object(fp, "exec_in", side_effect=lambda *a, **k: next(replies)),
            mock.patch("time.sleep"),
        ):
            fp.flush_lazyfs_cache("node2", mount=fp.LAZYFS_DATA, timeout_s=5.0)

    def test_a_submitted_checkpoint_that_never_lands_is_refused(self) -> None:
        with self.assertRaises(fp.FaultEffectNotObserved) as caught:
            self.flush("[lazyfs.cmds]: cache checkpoint request submitted...\n")
        self.assertIn("un-fsynced writes", str(caught.exception))

    def test_a_fifo_that_cannot_be_written_is_refused(self) -> None:
        with self.assertRaises(fp.FaultInjectionError):
            self.flush("", sent_rc=1)

    def test_the_command_word_is_the_one_lazyfs_parses(self) -> None:
        self.assertEqual(fp.lazyfs_checkpoint_cmd(), "lazyfs::cache-checkpoint")


class LazyfsConfigTests(unittest.TestCase):
    """The shipped LazyFS config, checked against what the harness assumes.

    These read the real file rather than a fixture: a config that drifts from
    the constants in fault_primitives is how the fault worker went silent, and
    the drift is invisible until a durability run reports perfect results.
    """

    @property
    def config(self) -> str:
        return (Path(__file__).resolve().parent / "lazyfs" / "lazyfs.toml").read_text()

    def test_completed_fifo_stays_unset(self) -> None:
        """Setting it parks the fault worker in open(O_WRONLY) on a FIFO no
        process opens for reading, which silently disables every fault."""
        for line in self.config.splitlines():
            self.assertFalse(
                line.strip().startswith("fifo_path_completed"),
                "fifo_path_completed must stay unset; it parks the LazyFS fault worker",
            )

    def test_config_paths_match_the_harness_constants(self) -> None:
        config = tomllib.loads(self.config)
        self.assertEqual(config["faults"]["fifo_path"], fp.LAZYFS_FIFO)
        self.assertEqual(config["filesystem"]["logfile"], fp.LAZYFS_LOG)

    def test_logfile_is_set_so_the_worker_can_be_observed(self) -> None:
        """With no logfile LazyFS logs to the console only, and the harness has
        no way to tell a consumed command from an unread one."""
        config = tomllib.loads(self.config)
        self.assertNotEqual(config["filesystem"]["logfile"], "")


DAEMON_RESTARTING = (
    "Error response from daemon: Container "
    "c5bfb91e7a56c74cddfcdbf65701fc85122750713fcb16009f6b60b63c5c846a is restarting, "
    "wait until the container is running"
)
DAEMON_STOPPED = "Error response from daemon: Container 8823f52780b2 is not running"
DAEMON_NO_SUCH = "Error response from daemon: No such container: node9"
SETNS_FAILURE = (
    "OCI runtime exec failed: exec failed: unable to start container process: "
    "error executing setns process: exit status 1"
)
OCI_STOPPED = "OCI runtime exec failed: exec failed: cannot exec in a stopped container"

LAZYFS_MOUNTS = (
    "proc /proc proc rw,relatime 0 0\n"
    "/dev/vda1 / ext4 rw,relatime 0 0\n"
    "lazyfs /var/lib/postgresql/data fuse rw,nosuid,nodev,relatime,user_id=0 0 0\n"
)
PLAIN_MOUNTS = (
    "proc /proc proc rw,relatime 0 0\n/dev/vda1 /var/lib/postgresql/data ext4 rw,relatime 0 0\n"
)


class SequencedRunner:
    """Returns each queued result in turn, repeating the last forever.

    ``ScriptedRunner`` answers a command the same way every time, which cannot
    express a container that is unreachable now and reachable a moment later.
    """

    def __init__(self, results: Sequence[CommandResult]) -> None:
        self.results = list(results)
        self.calls = 0

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        index = min(self.calls, len(self.results) - 1)
        self.calls += 1
        return self.results[index]


class WipeNodeStateTests(RunnerFixture):
    """The wipe has to be read back. A removal that silently did nothing leaves
    the node rejoining with all its state, which is a green run measuring a
    fault that never happened."""

    def test_a_wipe_that_left_entries_behind_fails(self) -> None:
        runner = self.install(ScriptedRunner([("docker compose run", ok("PG_VERSION\nbase\n"))]))
        with self.assertRaises(FaultEffectNotObserved) as caught:
            fp.wipe_node_state("node2", ["/var/lib/postgresql/data"])
        self.assertIn("still hold entries", str(caught.exception))
        self.assertTrue(runner.matching("docker compose stop node2"))

    def test_an_empty_read_back_is_a_wipe(self) -> None:
        self.install(ScriptedRunner([("docker compose run", ok("  \n"))]))
        fp.wipe_node_state("node2", ["/var/lib/postgresql/data"])
        self.assertTrue(any(e["event"] == "fault.injected" for e in self.events))

    def test_a_node_that_will_not_stop_is_not_wiped(self) -> None:
        runner = self.install(ScriptedRunner([("docker compose stop", fail("no such service"))]))
        with self.assertRaises(fp.FaultInjectionError):
            fp.wipe_node_state("node2", ["/var/lib/postgresql/data"])
        self.assertEqual(runner.matching("docker compose run"), [])

    def test_every_named_path_is_removed_and_checked(self) -> None:
        runner = self.install(ScriptedRunner([("docker compose run", ok(""))]))
        fp.wipe_node_state("node2", ["/var/lib/postgresql/data", "/var/lib/postgresql/raft"])
        script = runner.matching("docker compose run")[0]
        self.assertIn("/var/lib/postgresql/data/*", script)
        self.assertIn("/var/lib/postgresql/raft/*", script)
        # One `ls -A` per directory. Passing several at once makes `ls` print a
        # `dir:` header before each listing, so the read-back is never empty and
        # a multi-path wipe can never be verified — every one would fail as
        # "still hold entries" with the header as the evidence.
        self.assertIn('ls -A "/var/lib/postgresql/data"', script)
        self.assertIn('ls -A "/var/lib/postgresql/raft"', script)
        self.assertNotIn("ls -A /var/lib/postgresql/data /var/lib/postgresql/raft", script)

    def test_a_multi_path_wipe_can_read_back_empty(self) -> None:
        """The regression: two paths must be verifiable, not just one."""
        self.install(ScriptedRunner([("docker compose run", ok(""))]))
        fp.wipe_node_state("node2", ["/var/lib/postgresql/data", "/var/lib/postgresql/raft"])
        self.assertTrue(any(e["event"] == "fault.injected" for e in self.events))


class ContainerReachabilityTests(unittest.TestCase):
    """A container that cannot be exec'd into has told us nothing.

    Both durability suites failed in CI on readers that could not distinguish
    "I could not look" from "I looked and the answer is no".
    """

    def install(self, runner: SequencedRunner) -> SequencedRunner:
        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        return runner

    def test_a_restarting_container_makes_a_read_indeterminate(self) -> None:
        self.install(SequencedRunner([fail(DAEMON_RESTARTING)]))
        with self.assertRaises(fp.ContainerNotRunning):
            fp.read_processes("node2")

    def test_a_stopped_container_makes_a_read_indeterminate(self) -> None:
        self.install(SequencedRunner([fail(DAEMON_STOPPED)]))
        with self.assertRaises(fp.ContainerNotRunning):
            fp.read_processes("node2")

    def test_the_oci_wording_for_a_stopped_container_is_indeterminate_too(self) -> None:
        """The daemon's phrasing is not the only one: on a container that has
        not started yet the OCI runtime's own message comes through instead, and
        reading it as a genuine failure is what stopped the torn-raft suite
        waiting for its cluster and failed it on the mount check."""
        self.install(SequencedRunner([fail(OCI_STOPPED)]))
        with self.assertRaises(fp.ContainerNotRunning):
            fp.read_processes("node2")

    def test_a_silent_non_zero_exit_is_indeterminate(self) -> None:
        """docker exiting non-zero with nothing on either stream. A command that
        genuinely ran and failed reports why, so silence is docker's, not the
        command's — this is the shape that failed torn-raft in CI."""
        self.install(SequencedRunner([CommandResult(1, "", "")]))
        with self.assertRaises(fp.ContainerNotRunning):
            fp.read_processes("node2")

    def test_a_timeout_is_indeterminate(self) -> None:
        self.install(SequencedRunner([CommandResult(fp.TIMED_OUT_RC, "", "timeout after 15.0s")]))
        with self.assertRaises(fp.ContainerNotRunning):
            fp.read_processes("node2")

    def test_a_command_that_ran_and_failed_is_not_indeterminate(self) -> None:
        """``ps`` exiting non-zero inside a running container is a real fault."""
        self.install(SequencedRunner([fail("ps: unrecognized option '--sort'")]))
        with self.assertRaises(FaultPreconditionError) as caught:
            fp.read_processes("node2")
        self.assertNotIsInstance(caught.exception, fp.ContainerNotRunning)

    def test_a_container_that_does_not_exist_is_not_indeterminate(self) -> None:
        """A name resolving to nothing is topology drift, not a state to wait
        through."""
        self.install(SequencedRunner([fail(DAEMON_NO_SUCH)]))
        with self.assertRaises(FaultPreconditionError) as caught:
            fp.read_processes("node9")
        self.assertNotIsInstance(caught.exception, fp.ContainerNotRunning)

    def test_exec_when_deliverable_waits_out_an_undelivered_exec(self) -> None:
        runner = self.install(
            SequencedRunner([fail(SETNS_FAILURE), CommandResult(137, "", ""), ok("done")])
        )
        with mock.patch("time.sleep"):
            result = fp.exec_when_deliverable("node2", "dd if=/dev/urandom of=x", timeout_s=30.0)
        self.assertTrue(result.ok)
        self.assertEqual(runner.calls, 3)

    def test_exec_when_deliverable_returns_a_command_that_really_failed(self) -> None:
        """It waits for the exec to land, not for the command to succeed."""
        runner = self.install(SequencedRunner([fail("dd: No space left on device")]))
        with mock.patch("time.sleep"):
            result = fp.exec_when_deliverable("node2", "dd if=/dev/urandom of=x", timeout_s=30.0)
        self.assertFalse(result.ok)
        self.assertEqual(runner.calls, 1)

    def test_exec_when_deliverable_gives_up_carrying_the_last_reason(self) -> None:
        self.install(SequencedRunner([fail(SETNS_FAILURE)]))
        with mock.patch("time.sleep"):
            result = fp.exec_when_deliverable("node2", "true", timeout_s=1.0)
        self.assertFalse(result.ok)
        self.assertIn("setns", result.output)

    def test_the_mount_wait_outlasts_a_restarting_container(self) -> None:
        runner = self.install(
            SequencedRunner([fail(DAEMON_RESTARTING), fail(DAEMON_RESTARTING), ok(LAZYFS_MOUNTS)])
        )
        with mock.patch("time.sleep"):
            fp.verify_lazyfs_mounted("node2", "/var/lib/postgresql/data", timeout_s=30.0)
        self.assertEqual(runner.calls, 3)

    def test_a_container_that_never_runs_is_reported_as_such(self) -> None:
        """Not as an unmounted one — the two need different things looked at."""
        self.install(SequencedRunner([fail(DAEMON_RESTARTING)]))
        with mock.patch("time.sleep"), self.assertRaises(FaultPreconditionError) as caught:
            fp.verify_lazyfs_mounted("node2", "/var/lib/postgresql/data", timeout_s=2.0)
        message = str(caught.exception)
        self.assertIn("is restarting", message)
        self.assertNotIn("is not a LazyFS mount", message)

    def test_a_running_container_without_the_mount_still_fails(self) -> None:
        self.install(SequencedRunner([ok(PLAIN_MOUNTS)]))
        with mock.patch("time.sleep"), self.assertRaises(FaultPreconditionError) as caught:
            fp.verify_lazyfs_mounted("node2", "/var/lib/postgresql/data", timeout_s=2.0)
        self.assertIn("is not a LazyFS mount", str(caught.exception))


class StallReasonTests(unittest.TestCase):
    """What a harness that gave up waiting says about why. Reported to a human
    reading a failed CI job, so a line that names a routine event instead of the
    reason sends the next hour in the wrong direction."""

    def test_a_torn_clone_is_named(self) -> None:
        log = (
            "node3-1  | waiting for checkpoint\n"
            "node3-1  | pg_basebackup: error: unexpected termination of "
            "replication stream: ERROR:  could not read from WAL segment\n"
            "node3-1  | Error: pg_basebackup failed with exit code: Some(1)\n"
        )
        reason = fp.stall_reason(log)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("unexpected termination of replication stream", reason)

    def test_a_dropped_client_does_not_outrank_the_real_reason(self) -> None:
        """The most recent match is the one reported, and PostgreSQL logs a
        FATAL every time a client goes away — which is how a node whose clone
        kept failing was reported as having lost a connection."""
        log = (
            "node1-1  | pg_basebackup: error: unexpected termination of replication stream\n"
            "node1-1  | 2026-08-21 17:57:04 UTC [555] FATAL:  connection to client lost\n"
        )
        reason = fp.stall_reason(log)
        assert reason is not None
        self.assertIn("pg_basebackup", reason)

    def test_a_log_of_nothing_but_routine_lines_has_no_reason(self) -> None:
        log = (
            "node2-1  | 2026-08-21 17:56:47 UTC [761] FATAL:  the database system is starting up\n"
            "node2-1  | 2026-08-21 17:56:48 UTC [762] FATAL:  connection to client lost\n"
        )
        self.assertIsNone(fp.stall_reason(log))

    def test_a_real_fatal_is_still_reported(self) -> None:
        log = "node2-1  | 2026-08-21 17:56:47 UTC [761] FATAL:  Data directory is not empty\n"
        reason = fp.stall_reason(log)
        assert reason is not None
        self.assertIn("is not empty", reason)


if __name__ == "__main__":
    unittest.main()
