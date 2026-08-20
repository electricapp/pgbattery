#!/usr/bin/env bash
#
# Build the cluster images from a binary cross-compiled on the host.
#
# The in-VM build is slow for a structural reason rather than a tunable one:
# `COPY . .` gives cargo fresh mtimes on every file, so it refingerprints and
# rebuilds all three workspace crates even when one line changed. A host build
# keeps its own fingerprints and rebuilds only what moved — measured at 38s
# against 2m11 for the same edit.
#
# Usage:
#   scripts/dev-image.sh                     build the images
#   scripts/dev-image.sh && ./testing/ci_runner.py --suite ha-parallel \
#       --case <id> --no-build               ... and run a case against them
#
# `--no-build` matters: without it the runner rebuilds in the VM and throws the
# fast image away. Nothing here is used by CI, which always builds in Docker.

set -euo pipefail

cd "$(dirname "$0")/.."

case "$(uname -m)" in
  arm64 | aarch64) target=aarch64-unknown-linux-gnu ;;
  x86_64) target=x86_64-unknown-linux-gnu ;;
  *)
    echo "dev-image: no linux target known for $(uname -m)" >&2
    exit 1
    ;;
esac

for tool in zig cargo-zigbuild jq; do
  command -v "$tool" >/dev/null || {
    echo "dev-image: $tool is not installed (brew install zig; cargo install cargo-zigbuild)" >&2
    exit 1
  }
done

# Pinned to the runtime image's glibc floor rather than the host's: zig targets
# whatever version is named, and naming none produces a binary that may not load
# in postgres:18.
glibc=2.36

echo "dev-image: cross-building for $target (glibc $glibc)"
cargo zigbuild --profile ci --locked --target "$target.$glibc" --bin pgbattery

mkdir -p .devbin
cp "target/$target/ci/pgbattery" .devbin/pgbattery

echo "dev-image: building runtime-prebuilt from .devbin/pgbattery"
docker build --target runtime-prebuilt -t pgbattery-dev-prebuilt .

# Every service in the compose file runs the same image; compose names a built
# image `<project>-<service>` and does not write the name into its config, so
# build it from compose's own resolved project and service names. Derived rather
# than restated for the same reason topology.py reads the compose file: CI sets
# a per-run COMPOSE_PROJECT_NAME, and a hardcoded name would tag an image
# nothing runs while the run silently tested the previous binary.
images=$(docker compose config --format json |
  jq -r '.name as $p | .services | to_entries[] | select(.value.build) | "\($p)-\(.key)"')

if [ -z "$images" ]; then
  echo "dev-image: compose reported no buildable services — refusing to leave stale images in place" >&2
  exit 1
fi

for image in $images; do
  docker tag pgbattery-dev-prebuilt "$image"
  echo "  tagged $image"
done

echo "dev-image: done — run cases with --no-build so they use these images"
