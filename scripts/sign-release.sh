#!/usr/bin/env bash
# Sign pgbattery release artifacts with Sigstore **cosign keyless**: writes a
# .sha256, a .bundle (Sigstore bundle: signature + cert + Rekor inclusion
# proof), a .sig (signature), and a .pem (Fulcio certificate) next to every
# `pgbattery-*` binary in the given directory.
#
# In CI this is done inline by .github/workflows/release.yml (GitHub Actions
# OIDC → Fulcio, no prompts). This script is for **local / manual** signing,
# where cosign will open a browser for the OIDC flow. Keyless signing needs NO
# secret key — trust is anchored in the Sigstore/Fulcio root plus the verified
# identity (see docs/RELEASING.md).
#
# Usage:
#   scripts/sign-release.sh dist/
#
# Requires: cosign (https://docs.sigstore.dev/), sha256sum or shasum.
set -euo pipefail

DIR="${1:?usage: sign-release.sh <artifact-dir>}"

if ! command -v cosign >/dev/null 2>&1; then
  echo "error: cosign not found on PATH (https://docs.sigstore.dev/system_config/installation/)" >&2
  exit 1
fi

# Identity to verify against after signing. Defaults to this repo's release
# workflow; override for local experimentation. The issuer is GitHub Actions in
# CI; a local browser-based signon uses a different issuer, so verification is
# skipped unless IDENTITY_REGEX + OIDC_ISSUER are both provided.
OIDC_ISSUER="${OIDC_ISSUER:-}"
IDENTITY_REGEX="${IDENTITY_REGEX:-}"

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
    *.sha256 | *.bundle | *.sig | *.pem) continue ;;
  esac
  echo "signing $f"
  sha256 "$f" > "$f.sha256"
  # Emit BOTH a Sigstore bundle (signature + cert + Rekor inclusion proof, the
  # primary artifact the client verifies) and the detached sig/cert fallback.
  cosign sign-blob --yes \
    --bundle "$f.bundle" \
    --output-signature "$f.sig" \
    --output-certificate "$f.pem" \
    "$f"
  # Verify immediately when an identity policy is supplied (always the case in
  # CI; optional locally). A failed verify aborts (set -e).
  if [[ -n "$OIDC_ISSUER" && -n "$IDENTITY_REGEX" ]]; then
    cosign verify-blob \
      --signature "$f.sig" \
      --certificate "$f.pem" \
      --certificate-oidc-issuer "$OIDC_ISSUER" \
      --certificate-identity-regexp "$IDENTITY_REGEX" \
      "$f"
  else
    echo "  (skipping verify: set OIDC_ISSUER + IDENTITY_REGEX to verify identity)"
  fi
  signed=$((signed + 1))
done

if [[ "$signed" -eq 0 ]]; then
  echo "error: no pgbattery-* artifacts found in $DIR" >&2
  exit 1
fi
echo "signed $signed artifact(s) (.sha256 + .bundle + .sig + .pem)"
