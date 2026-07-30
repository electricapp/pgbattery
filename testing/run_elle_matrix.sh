#!/usr/bin/env bash
# Run Elle × attack matrix.
#
# Usage:
#   testing/run_elle_matrix.sh                   # every attack in ALL_ATTACKS (16)
#   testing/run_elle_matrix.sh kill              # single attack
#   testing/run_elle_matrix.sh kill partition    # subset
#   testing/run_elle_matrix.sh --list-attacks    # print ALL_ATTACKS, one per line
#
# Environment:
#   ELLE_PROFILE=full|smoke  Density preset. `full` (default) is the
#                            exploratory config: 6 workers, 3 keys, 90 s, 4
#                            fault instants. `smoke` is the ~45 s PR-path
#                            config: 4 workers, 3 keys, 30 s, 1 fault.
#   ELLE_SEED=<int>          Pin the worker RNG seed to replay one run. Default
#                            is a fresh random seed per attack per run.
#   ELLE_SHARD=i/n           Run only shard i of n, slicing ALL_ATTACKS
#                            round-robin. Ignored when attacks are named.
#   ELLE_WORKLOAD, ELLE_WORKERS, ELLE_KEYS, ELLE_DURATION, ELLE_FAULT_AT,
#   ELLE_FAULT_WAVES, ELLE_TIMEOUT   Override individual knobs.
#   ELLE_SKIP_BUILD=1        Trust an already-present uberjar.
#   ELLE_MIN_OK_PER_WORKER_SECOND
#                            Lower the aggregator's committed-transaction floor.
#                            Needed on Docker Desktop, whose loopback proxy runs
#                            this workload ~50x slower than CI.
#
# Each attack produces testing/artifacts/elle-<attack>/ with:
#   matrix_meta.json     — seed, density, planned fault schedule. Written
#                          BEFORE the run, so even a crashed run is replayable.
#   harness.log          — full harness console output
#   fault_waves.json     — what the extra fault waves actually injected
#   history.json         — raw Op dump (empty for txn / list-append workloads)
#   history.elle.json    — Elle-formatted history
#   elle_result.json     — parsed ElleResult
#   history_stats.json   — record-type counts (written by the aggregator)
#   results.json         — overall verdict + per-attack metadata
#   elle_stderr.log      — JVM stderr (debugging)
#
# Exit code 0 if every attack passes; 1 if any attack found an anomaly;
# 2 if any attack hit infrastructure failure (which includes "the history was
# too thin to falsify anything" — see aggregate_elle_results.py).

set -eu

ALL_ATTACKS=(
  kill
  partition
  freeze
  transfer
  cascade
  quorum_loss
  asymmetric_partition
  network_slow
  network_loss
  clock_skew
  pg_only_kill
  disk_full
  fsync_stall
  flap_partition
  membership_change
  chaos_storm
)

# Answered before anything else so the CI verdict job can ask for the canonical
# attack list without a JDK, docker, or a built uberjar.
if [ "${1:-}" = "--list-attacks" ]; then
  printf '%s\n' "${ALL_ATTACKS[@]}"
  exit 0
fi

# Ensure the Elle uberjar exists (build it on first run, or whenever the
# shim source has changed). The script is idempotent and a no-op when
# everything is already built.
#
# ELLE_SKIP_BUILD=1: trust an already-present uberjar and skip build_elle.sh.
# CI splits build (build-elle-jar job, has lein+JDK) from run (matrix job, only
# downloads the jar artifact). The matrix job has no lein, and build_elle.sh's
# freshness stamp isn't part of the artifact, so calling it there would try to
# rebuild and fail with "leiningen not found". The downloaded jar is
# authoritative; just verify it's present.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="$SCRIPT_DIR/third_party/elle/elle-cli-standalone.jar"
if [ "${ELLE_SKIP_BUILD:-0}" = "1" ]; then
  if [ ! -f "$JAR_PATH" ]; then
    echo "[ERR] ELLE_SKIP_BUILD=1 but uberjar is missing: $JAR_PATH" >&2
    exit 2
  fi
  echo "[OK] ELLE_SKIP_BUILD=1 — using prebuilt uberjar at $JAR_PATH"
else
  "$SCRIPT_DIR/build_elle.sh"
fi

# ── Attack selection ─────────────────────────────────────────────────────────
# Sharding slices ALL_ATTACKS round-robin, so the union of shards 1..n is
# exactly the full matrix by construction — no attack list is duplicated in
# YAML where it could drift, and no attack can be dropped without the shard
# banner below saying so. Round-robin (not contiguous) spreads the expensive
# attacks (chaos_storm, flap_partition) across shards.
if [ "$#" -gt 0 ]; then
  ATTACKS=("$@")
  if [ -n "${ELLE_SHARD:-}" ]; then
    echo "[WARN] ELLE_SHARD=${ELLE_SHARD} ignored — attacks were named on the command line"
  fi
elif [ -n "${ELLE_SHARD:-}" ]; then
  case "$ELLE_SHARD" in
    [0-9]*/[0-9]*) ;;
    *)
      echo "[ERR] ELLE_SHARD must be 'i/n' with integers, got '${ELLE_SHARD}'" >&2
      exit 2
      ;;
  esac
  shard_index="${ELLE_SHARD%%/*}"
  shard_count="${ELLE_SHARD##*/}"
  case "${shard_index}${shard_count}" in
    *[!0-9]*)
      echo "[ERR] ELLE_SHARD must be 'i/n' with integers, got '${ELLE_SHARD}'" >&2
      exit 2
      ;;
  esac
  if [ "$shard_count" -lt 1 ] || [ "$shard_index" -lt 1 ] || [ "$shard_index" -gt "$shard_count" ]; then
    echo "[ERR] ELLE_SHARD='${ELLE_SHARD}' out of range (need 1 <= i <= n)" >&2
    exit 2
  fi
  ATTACKS=()
  idx=0
  for a in "${ALL_ATTACKS[@]}"; do
    if [ $(( idx % shard_count + 1 )) -eq "$shard_index" ]; then
      ATTACKS+=("$a")
    fi
    idx=$(( idx + 1 ))
  done
  if [ "${#ATTACKS[@]}" -eq 0 ]; then
    echo "[ERR] shard ${shard_index}/${shard_count} is empty — more shards than the" \
      "${#ALL_ATTACKS[@]} attacks in ALL_ATTACKS" >&2
    exit 2
  fi
  echo "[INFO] shard ${shard_index}/${shard_count} runs ${#ATTACKS[@]} of ${#ALL_ATTACKS[@]} attacks:" \
    "${ATTACKS[*]}"
  echo "[INFO] the remaining $(( ${#ALL_ATTACKS[@]} - ${#ATTACKS[@]} )) attacks run in the other" \
    "$(( shard_count - 1 )) shard(s) of this workflow; coverage is complete only across all shards"
else
  ATTACKS=("${ALL_ATTACKS[@]}")
fi

# ── Density ──────────────────────────────────────────────────────────────────
# Elle finds anomalies by detecting cycles in a dependency graph, so the two
# things that matter are (a) how many committed transactions contend for the
# same key, and (b) how many of them are in flight across a leadership
# transition. `full` raises both: workers up and keys down (a 6-worker,
# 3-key list-append workload puts ~4 concurrent transactions on every key,
# vs ~1.6 at 4 workers / 5 keys), and four fault instants instead of one.
#
# Workers come in multiples of 3 because the harness pins worker i to gateway
# port i % 3; an uneven count loads one gateway (and one node) harder than the
# others and biases which workers get wedged when a node dies.
PROFILE="${ELLE_PROFILE:-full}"
case "$PROFILE" in
  full)
    P_WORKERS=6
    P_KEYS=3
    P_DURATION=90
    P_WAVES=4
    ;;
  smoke)
    P_WORKERS=4
    P_KEYS=3
    P_DURATION=30
    P_WAVES=1
    ;;
  *)
    echo "[ERR] unknown ELLE_PROFILE='${PROFILE}' (expected 'full' or 'smoke')" >&2
    exit 2
    ;;
esac

WORKLOAD="${ELLE_WORKLOAD:-list-append}"
WORKERS="${ELLE_WORKERS:-$P_WORKERS}"
KEYS="${ELLE_KEYS:-$P_KEYS}"
DURATION="${ELLE_DURATION:-$P_DURATION}"
FAULT_WAVES="${ELLE_FAULT_WAVES:-$P_WAVES}"
# 8 s = four lease durations of clean workload before the first fault, so every
# key already has a committed prefix for Elle to hang dependency edges on.
FAULT_AT="${ELLE_FAULT_AT:-8}"
TIMEOUT="${ELLE_TIMEOUT:-600}"

# ── Fault schedule ───────────────────────────────────────────────────────────
# Wave 1 is the harness's own injector (--fault-at). Waves 2..N are driven by
# the sibling process below, which calls the same ATTACK_DISPATCH entry so the
# whole schedule lands inside ONE history.
#
# Every gap is derived from the cluster's own timing constants:
#   lease duration        2 s                        src/governor/lease.rs:22
#   election timeout      1 s, openraft window [1,2] src/config/constants.rs:72
#   quorum detection      1 s                        src/config/constants.rs:100
#   leaderless watchdog   (5 + 8*rank) timeouts      src/config/constants.rs:134,144
#   replica disconnect    30 s                       src/config/constants.rs:62
#   LSN staleness         30 s                       src/config/constants.rs:214
#
# OVERLAP_GAP — lease (2 s) + one election timeout (1 s). Deliberately shorter
#   than any recovery path: the second fault lands while write authority is
#   still moving, so the cluster is provably NOT recovered. This is the
#   overlapping-stress pair, and the densest boundary the matrix produces.
# RECOVER_GAP — worst-case leadership recovery when openraft's own timers do
#   not converge and the leaderless watchdog has to drive it: rank-1 fires at
#   (5 + 8) = 13 election timeouts = 13 s, plus the election window (2 s) plus
#   the lease grant (2 s) = 17 s, rounded up to 20 s. A fault this far after
#   the previous one hits a genuinely recovered cluster.
# SYNC_DROP_GAP — 34 s, past both REPLICA_DISCONNECT_TIMEOUT_MS and
#   LSN_STALENESS_THRESHOLD_SECS (30 s each). A node still down from the
#   previous fault has by now been dropped from synchronous_standby_names and
#   its LSN has gone stale, so this wave lands on a cluster that has degraded
#   from SYNC to ASYNC durability — a regime one fault at t=8 never reaches.
# TAIL_MARGIN — one RECOVER_GAP of workload after the last fault, so the
#   history ends with recovered, committed operations. Elle needs those to
#   close cycles that opened inside a fault window.
OVERLAP_GAP=3
RECOVER_GAP=20
SYNC_DROP_GAP=34
TAIL_MARGIN=20
# Bound on how long the wave driver waits for the harness to reach its
# workload phase: wait_cluster_healthy(120) + table setup + interpreter start.
MARKER_TIMEOUT=150

duration_int="${DURATION%%.*}"
extra_waves=()
wave_t="${FAULT_AT%%.*}"
wave_n=1
dropped_waves=0
while [ "$wave_n" -lt "$FAULT_WAVES" ]; do
  case $(( (wave_n - 1) % 3 )) in
    0) gap="$OVERLAP_GAP"; mode=overlap ;;
    1) gap="$RECOVER_GAP"; mode=recover ;;
    *) gap="$SYNC_DROP_GAP"; mode=recover ;;
  esac
  wave_t=$(( wave_t + gap ))
  if [ $(( wave_t + TAIL_MARGIN )) -gt "$duration_int" ]; then
    dropped_waves=$(( FAULT_WAVES - wave_n ))
    echo "[WARN] fault waves $(( wave_n + 1 ))..${FAULT_WAVES} dropped: wave at t=${wave_t}s needs" \
      "ELLE_DURATION >= $(( wave_t + TAIL_MARGIN ))s, have ${duration_int}s." \
      "Coverage is reduced — raise ELLE_DURATION or lower ELLE_FAULT_WAVES." >&2
    break
  fi
  extra_waves+=("${wave_t}:${mode}")
  wave_n=$(( wave_n + 1 ))
done

fault_offsets="${FAULT_AT}(harness)"
if [ "${#extra_waves[@]}" -gt 0 ]; then
  for spec in "${extra_waves[@]}"; do
    fault_offsets="${fault_offsets} ${spec%%:*}(${spec##*:})"
  done
fi

random_seed() {
  # 31-bit seed from the kernel CSPRNG. $RANDOM is only 15 bits and awk's
  # srand() is seeded from time(), which collides for attacks that start in
  # the same second.
  od -An -N4 -tu4 < /dev/urandom | tr -d ' \n' | awk '{printf "%d\n", $1 % 2147483646 + 1}'
}

recover_cluster() {
  # Best-effort residue scrub. A normal harness run cleans up after itself, but
  # a `timeout`-killed harness (or a wave driver killed mid-fault) never
  # reaches its own cleanup, and leftover iptables rules / tc qdiscs / skewed
  # clocks / stopped containers would poison every remaining attack.
  echo "  [INFO] scrubbing fault residue before the next attack"
  PYTHONPATH="$SCRIPT_DIR" uv run --project testing python - <<'PY' || true
import linearizability_register as harness

harness.start_killed_nodes()
harness.scrub_chaos_residue()
PY
}

git_sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

if [ -n "${ELLE_SEED:-}" ]; then
  seed_policy="pinned to ${ELLE_SEED} for every attack (replay mode)"
else
  seed_policy="fresh random per attack, printed below and recorded in matrix_meta.json"
fi

echo "═══ Elle matrix configuration ═══"
echo "  profile:   $PROFILE"
echo "  workload:  $WORKLOAD (workers=$WORKERS keys=$KEYS duration=${DURATION}s)"
echo "  faults:    $(( ${#extra_waves[@]} + 1 )) per run at t = ${fault_offsets}"
echo "  seeds:     $seed_policy"
echo "  attacks:   ${#ATTACKS[@]} (${ATTACKS[*]})"
echo "  per-attack timeout: ${TIMEOUT}s"

overall_rc=0

for attack in "${ATTACKS[@]}"; do
  seed="${ELLE_SEED:-$(random_seed)}"
  artifact_dir="testing/artifacts/elle-$attack"
  log="$artifact_dir/harness.log"
  replay="ELLE_SEED=$seed ELLE_PROFILE=$PROFILE testing/run_elle_matrix.sh $attack"

  echo
  echo "═══ Elle × $attack (seed=$seed, profile=$PROFILE) ═══"
  rm -rf "$artifact_dir"
  mkdir -p "$artifact_dir"

  # Written before the run so a crash, timeout, or OOM still leaves behind the
  # seed and density needed to reproduce it.
  schedule_json="{\"offset_s\": ${FAULT_AT}, \"mode\": \"harness\"}"
  if [ "${#extra_waves[@]}" -gt 0 ]; then
    for spec in "${extra_waves[@]}"; do
      schedule_json="${schedule_json}, {\"offset_s\": ${spec%%:*}, \"mode\": \"${spec##*:}\"}"
    done
  fi
  cat > "$artifact_dir/matrix_meta.json" <<META
{
  "attack": "$attack",
  "seed": $seed,
  "seed_pinned": $([ -n "${ELLE_SEED:-}" ] && echo true || echo false),
  "replay": "$replay",
  "profile": "$PROFILE",
  "workload": "$WORKLOAD",
  "workers": $WORKERS,
  "keys": $KEYS,
  "duration_s": $DURATION,
  "fault_at_s": $FAULT_AT,
  "fault_waves_planned": $(( ${#extra_waves[@]} + 1 )),
  "fault_waves_dropped": $dropped_waves,
  "fault_schedule": [$schedule_json],
  "timeout_s": $TIMEOUT,
  "git_sha": "$git_sha",
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
META

  # Extra fault waves run in a sibling process because the harness's injector
  # fires exactly once. It syncs on the harness's own "Running workload"
  # banner rather than guessing how long cluster-health + table setup took,
  # and records every injection in fault_waves.json so a green run cannot
  # silently mean "the faults never landed".
  wave_pid=""
  PYTHONPATH="$SCRIPT_DIR" uv run --project testing python - \
    "$attack" "$log" "$artifact_dir" "$MARKER_TIMEOUT" \
    ${extra_waves[@]+"${extra_waves[@]}"} <<'PY' &
"""Inject Elle-matrix fault waves 2..N for one attack.

The harness (linearizability_register.py) injects exactly one fault, at
--fault-at. This driver adds the later waves by calling the same
ATTACK_DISPATCH entry the harness would, from a sibling process, so a single
history spans several leadership transitions. Every fault function cleans up
its own scope in its own `finally`, which is what makes calling them from
here safe.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import linearizability_register as harness

WORKLOAD_MARKER = "Running workload for"
# Generous versus the 17 s worst-case leadership recovery derived in
# run_elle_matrix.sh, so a "recover" wave waits out even a watchdog-driven
# election before it fires.
RECOVER_WAIT_S = 45

attack = sys.argv[1]
log_path = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
marker_timeout = float(sys.argv[4])
planned: list[dict[str, object]] = [
    {"offset_s": float(spec.split(":")[0]), "mode": spec.split(":")[1]} for spec in sys.argv[5:]
]
injected: list[dict[str, object]] = []
notes: list[str] = []
marker_seen = False


def flush() -> None:
    """Rewrite fault_waves.json after every wave, so a killed driver still
    leaves evidence of what it managed to inject."""
    (out_dir / "fault_waves.json").write_text(
        json.dumps(
            {
                "attack": attack,
                "marker_seen": marker_seen,
                "planned": planned,
                "injected": injected,
                "notes": notes,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


flush()

if attack not in harness.ATTACK_DISPATCH:
    notes.append(f"unknown attack {attack!r}")
    flush()
    print(f"[ERR] fault-wave driver: unknown attack {attack!r}")
    raise SystemExit(2)

deadline = time.monotonic() + marker_timeout
while time.monotonic() < deadline:
    try:
        if WORKLOAD_MARKER in log_path.read_text(errors="replace"):
            marker_seen = True
            break
    except OSError:
        pass
    time.sleep(0.5)

if not marker_seen:
    notes.append(
        f"workload-start marker {WORKLOAD_MARKER!r} not seen within {marker_timeout:.0f}s; "
        "no extra faults injected"
    )
    flush()
    print(f"[ERR] fault-wave driver: {notes[-1]}")
    raise SystemExit(2)

t0 = time.monotonic()
print(f"[wave] workload started; {len(planned)} extra fault wave(s) queued for {attack}")

for wave in planned:
    offset = float(wave["offset_s"])
    mode = str(wave["mode"])
    remaining = t0 + offset - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    healthy: bool | None = None
    if mode == "recover":
        # Restart whatever the previous wave left down and wait for a leader,
        # so this fault hits a recovered cluster instead of piling onto a
        # broken one. `overlap` waves deliberately skip both.
        harness.start_killed_nodes()
        healthy = harness.wait_cluster_healthy(timeout=RECOVER_WAIT_S)

    leader_before, _ = harness.find_leader()
    at_s = time.monotonic() - t0
    error: str | None = None
    try:
        harness.ATTACK_DISPATCH[attack](0.0)
    except Exception as exc:  # noqa: BLE001 - recorded, never fatal to the run
        error = f"{type(exc).__name__}: {exc}"
    leader_after, _ = harness.find_leader()

    injected.append(
        {
            "offset_s": offset,
            "mode": mode,
            "injected_at_s": round(at_s, 2),
            "cluster_healthy_before": healthy,
            "leader_before": leader_before,
            "leader_after": leader_after,
            # A fault can only bite if there was a leader to aim at; a wave
            # that found none is recorded as attempted-but-ineffective rather
            # than silently counted as coverage.
            "effective": leader_before is not None and error is None,
            "error": error,
        }
    )
    flush()
    print(
        f"[wave] t={at_s:5.1f}s {mode:<7} {attack} "
        f"leader {leader_before} -> {leader_after}"
        + (f" ERROR {error}" if error else "")
    )

flush()
PY
  wave_pid=$!

  set +e
  timeout "$TIMEOUT" uv run --project testing testing/linearizability_register.py \
    --workload "$WORKLOAD" \
    --check elle \
    --attack "$attack" \
    --workers "$WORKERS" \
    --keys "$KEYS" \
    --duration "$DURATION" \
    --fault-at "$FAULT_AT" \
    --artifact-dir "$artifact_dir" \
    --seed "$seed" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  set -e

  # A healthy run's last wave fires a TAIL_MARGIN before the workload ends, so
  # the driver is already done here. It is still alive only when the harness
  # died early, in which case its remaining schedule would fire against a
  # cluster nobody is measuring.
  wave_killed=0
  if [ "$rc" -ne 0 ] && kill -0 "$wave_pid" 2>/dev/null; then
    echo "  [INFO] harness exited rc=$rc — stopping the fault-wave driver"
    kill "$wave_pid" 2>/dev/null || true
    wave_killed=1
  fi
  wave_rc=0
  wait "$wave_pid" || wave_rc=$?
  if [ "$wave_killed" -eq 1 ] && { [ "$wave_rc" -eq 143 ] || [ "$wave_rc" -eq 137 ]; }; then
    wave_rc=0
  fi

  case "$rc" in
    0) echo "  [PASS] $attack (seed=$seed)" ;;
    1) echo "  [FAIL] $attack - Elle found anomalies. Replay: $replay"
       overall_rc=$((overall_rc == 2 ? 2 : 1)) ;;
    124) echo "  [TIMEOUT] $attack - exceeded ${TIMEOUT}s. Replay: $replay"; overall_rc=2 ;;
    *) echo "  [ERROR] $attack - rc=$rc. Replay: $replay"; overall_rc=2 ;;
  esac

  if [ "$wave_rc" -ne 0 ]; then
    # The aggregator fails the run on this too (fault_waves.json is part of
    # the evidence it checks); surface it here as well so the console log of a
    # failing nightly names the cause at the point it happened.
    echo "  [ERROR] $attack - fault-wave driver rc=$wave_rc: extra faults did not all land." \
      "See $artifact_dir/fault_waves.json"
    overall_rc=2
  fi

  if [ "$rc" -ne 0 ] || [ "$wave_rc" -ne 0 ]; then
    recover_cluster
  fi
done

echo
echo "═══ Aggregating ═══"
set +e
ELLE_EXPECT_ATTACKS="${ATTACKS[*]}" \
  ELLE_EXPECT_PROFILE="$PROFILE" \
  uv run --project testing testing/aggregate_elle_results.py
agg_rc=$?
set -e
# The aggregator is the authority on evidence-level failures (missing
# artifacts, empty or implausibly thin histories, faults that never landed).
# Take the worse of the two verdicts; 2 (infrastructure) outranks 1 (anomaly).
if [ "$agg_rc" -gt "$overall_rc" ]; then
  overall_rc=$agg_rc
fi

exit "$overall_rc"
