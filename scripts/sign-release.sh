#!/usr/bin/env bash
# Sign pgbattery release artifacts: writes a .sha256 and a .minisig next to every
# `pgbattery-*` binary in the given directory.
#
# Usage:
#   MINISIGN_SECRET_KEY_FILE=pgbattery.key scripts/sign-release.sh dist/
#   # or, with the key inline (e.g. from a CI secret):
#   MINISIGN_SECRET_KEY="$(cat pgbattery.key)" scripts/sign-release.sh dist/
#
# Requires: minisign (https://jdsq.github.io/minisign/), sha256sum or shasum.
# The signing key should be password-less for non-interactive use (see
# docs/RELEASING.md); a password-protected key will prompt on the terminal.
set -euo pipefail

DIR="${1:?usage: sign-release.sh <artifact-dir>}"

if ! command -v minisign >/dev/null 2>&1; then
  echo "error: minisign not found on PATH (brew/apt install minisign)" >&2
  exit 1
fi

# Resolve the secret key into a file minisign can read.
KEY_FILE=""
CLEANUP=""
if [[ -n "${MINISIGN_SECRET_KEY_FILE:-}" ]]; then
  KEY_FILE="$MINISIGN_SECRET_KEY_FILE"
elif [[ -n "${MINISIGN_SECRET_KEY:-}" ]]; then
  KEY_FILE="$(mktemp)"
  CLEANUP="$KEY_FILE"
  printf '%s\n' "$MINISIGN_SECRET_KEY" > "$KEY_FILE"
else
  echo "error: set MINISIGN_SECRET_KEY_FILE or MINISIGN_SECRET_KEY" >&2
  exit 1
fi
trap '[[ -n "$CLEANUP" ]] && rm -f "$CLEANUP"' EXIT

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

shopt -s nullglob
signed=0
for f in "$DIR"/pgbattery-*; do
  case "$f" in
    *.sha256 | *.minisig) continue ;;
  esac
  echo "signing $f"
  sha256 "$f" > "$f.sha256"
  # -H: prehashed signature (the client verifies with allow_legacy = false).
  minisign -S -H -s "$KEY_FILE" -m "$f" -x "$f.minisig" -c "pgbattery release" -t "pgbattery $(basename "$f")"
  signed=$((signed + 1))
done

if [[ "$signed" -eq 0 ]]; then
  echo "error: no pgbattery-* artifacts found in $DIR" >&2
  exit 1
fi
echo "signed $signed artifact(s)"
