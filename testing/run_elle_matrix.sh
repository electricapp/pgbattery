#!/usr/bin/env bash
# Run Elle × attack matrix.
#
# Usage:
#   testing/run_elle_matrix.sh                   # all 7 attacks
#   testing/run_elle_matrix.sh kill              # single attack
#   testing/run_elle_matrix.sh kill partition    # subset
#
# Each invocation produces testing/artifacts/elle-<attack>/ with:
#   history.json         — raw Op dump
#   history.elle.json    — Elle-formatted history
#   elle_result.json     — parsed ElleResult
#   results.json         — overall verdict + per-attack metadata
#   elle_stderr.log      — JVM stderr (debugging)
#
# Exit code 0 if every attack passes; 1 if any attack found an anomaly;
# 2 if any attack hit infrastructure failure.

set -eu

# Ensure the Elle uberjar exists (build it on first run, or whenever the
# shim source has changed). The script is idempotent and a no-op when
# everything is already built.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/build_elle.sh"

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
ATTACKS=("${@:-${ALL_ATTACKS[@]}}")

WORKLOAD="${ELLE_WORKLOAD:-list-append}"
WORKERS="${ELLE_WORKERS:-4}"
KEYS="${ELLE_KEYS:-5}"
DURATION="${ELLE_DURATION:-30}"
FAULT_AT="${ELLE_FAULT_AT:-8}"
SEED="${ELLE_SEED:-42}"
TIMEOUT="${ELLE_TIMEOUT:-600}"

overall_rc=0

for attack in "${ATTACKS[@]}"; do
  echo
  echo "═══ Elle × $attack ═══"
  artifact_dir="testing/artifacts/elle-$attack"
  rm -rf "$artifact_dir"

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
    --seed "$SEED"
  rc=$?
  set -e

  case "$rc" in
    0) echo "  [PASS] $attack" ;;
    1) echo "  [FAIL] $attack - Elle found anomalies"; overall_rc=$((overall_rc == 2 ? 2 : 1)) ;;
    124) echo "  [TIMEOUT] $attack - exceeded ${TIMEOUT}s"; overall_rc=2 ;;
    *) echo "  [ERROR] $attack - rc=$rc"; overall_rc=2 ;;
  esac
done

echo
echo "═══ Aggregating ═══"
uv run --project testing testing/aggregate_elle_results.py

exit "$overall_rc"
