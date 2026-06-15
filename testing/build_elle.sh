#!/usr/bin/env bash
# Build the Elle uberjar from source. Idempotent: rebuilds only when the
# jar is missing OR the shim source / project.clj has changed.
#
# After this finishes, `testing/third_party/elle/elle-cli-standalone.jar`
# is ready to run via `java -jar ...`. The matrix runner and CI both
# delegate to this script.

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SHIM_DIR="$REPO_ROOT/testing/elle_shim"
JAR_PATH="$REPO_ROOT/testing/third_party/elle/elle-cli-standalone.jar"
STAMP_PATH="$REPO_ROOT/testing/third_party/elle/.build-stamp"

# Stamp = sha of project.clj + every .clj under src/. Rebuild iff stamp changes.
compute_stamp() {
  {
    cat "$SHIM_DIR/project.clj"
    find "$SHIM_DIR/src" -name '*.clj' -print0 | sort -z | xargs -0 cat
  } | shasum -a 256 | awk '{print $1}'
}

NEW_STAMP="$(compute_stamp)"
OLD_STAMP=""
if [ -f "$STAMP_PATH" ]; then
  OLD_STAMP="$(cat "$STAMP_PATH")"
fi

if [ -f "$JAR_PATH" ] && [ "$NEW_STAMP" = "$OLD_STAMP" ]; then
  echo "[OK] Elle uberjar up-to-date ($JAR_PATH)"
  exit 0
fi

if ! command -v lein >/dev/null 2>&1; then
  echo "[ERR] leiningen not found on PATH" >&2
  echo "  Install:  brew install leiningen" >&2
  echo "    (Linux)  curl -sSfL https://raw.githubusercontent.com/technomancy/leiningen/2.11.2/bin/lein -o /usr/local/bin/lein && chmod +x /usr/local/bin/lein" >&2
  exit 2
fi

if ! command -v java >/dev/null 2>&1; then
  echo "[ERR] java not found on PATH (need JDK 21+)" >&2
  echo "  Install:  brew install openjdk@21" >&2
  exit 2
fi

echo "==> Building Elle uberjar (shim source changed or jar missing)…"
mkdir -p "$(dirname "$JAR_PATH")"
(
  cd "$SHIM_DIR"
  lein clean
  lein uberjar
)

if [ ! -f "$JAR_PATH" ]; then
  echo "[ERR] Build completed but $JAR_PATH is missing" >&2
  exit 1
fi

echo "$NEW_STAMP" > "$STAMP_PATH"
echo "[OK] Built: $JAR_PATH"
