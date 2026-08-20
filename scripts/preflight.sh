#!/usr/bin/env bash
#
# Every CI gate that can run on a developer machine, run before the push that
# would otherwise discover it.
#
# This exists because the failures it catches are all of one kind: a lint that
# was verified locally by reading an empty grep, or by trusting a wrapper that
# summarised the compiler's output, and was therefore never verified at all. A
# script is the fix rather than more care, because it prints its own verdict and
# exits non-zero, which no summary can turn into silence.
#
# `.githooks/pre-push` runs this. `lint_matrix.py` checks that the commands
# below still match the workflows, so this cannot drift into mirroring a CI that
# no longer exists.
#
# Usage:
#   scripts/preflight.sh            every gate
#   scripts/preflight.sh --quick    skip the two slow compile-and-run gates
#   SKIP_PREFLIGHT=1 git push       bypass, for a push that cannot wait

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

failed=()
passed=0

# Run one gate. Output is shown only on failure: a passing gate that prints
# nothing is the point, and a wall of green output is how a real failure gets
# scrolled past.
gate() {
  local name="$1"
  shift
  local output
  local start
  start=$SECONDS
  if output=$("$@" 2>&1); then
    printf '  \033[32mok\033[0m   %-34s %ds\n' "$name" "$((SECONDS - start))"
    passed=$((passed + 1))
  else
    printf '  \033[31mFAIL\033[0m %-34s %ds\n' "$name" "$((SECONDS - start))"
    printf '%s\n' "$output" | sed 's/^/       /'
    failed+=("$name")
  fi
}

echo "preflight: the CI gates that run without a runner"

# Each gate names the CI job it stands in for. `lint_matrix.py` reconciles these
# against the workflows, so a lint job added to CI without a gate here fails the
# harness lint rather than waiting to fail a push — the same drift that left
# twelve ha-parallel cases run by nothing.
#
# The commands are CI's own, character for character, because the lint matches
# them that way. Anything reworded here to read better stops being the gate CI
# applies, which is the whole failure this file exists to prevent.
#
# mirrors: ci.yml:fmt
gate "cargo fmt" cargo fmt --all -- --check
# mirrors: ci.yml:clippy
gate "clippy" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
# mirrors: ci.yml:fuzz-clippy
gate "clippy (fuzz)" bash -c 'cd fuzz && cargo clippy --all-targets --locked -- -D warnings'
# mirrors: ci.yml:unused-deps
gate "cargo machete" cargo machete
# mirrors: ci.yml:typos
gate "typos" typos
# mirrors: prettier.yml:prettier
gate "prettier (markdown)" npx --yes prettier@3.8.4 --check "**/*.md"

# mirrors: ha-ci.yml:lint-test-harness
gate "ruff format" uv run --project testing ruff format --check testing/
# mirrors: ha-ci.yml:lint-test-harness
gate "ruff lint" uv run --project testing ruff check testing/
# mirrors: ha-ci.yml:lint-test-harness
gate "mypy strict" uv run --project testing mypy testing/ --strict
# mirrors: ha-ci.yml:lint-test-harness
gate "harness lint" uv run --python 3.14 --script testing/lint_matrix.py
# mirrors: ha-ci.yml:lint-test-harness
gate "clock injection lint" uv run --python 3.14 --script testing/lint_clock_injection.py

# The harness self-tests gate CI as one job; a single failure there is a
# checker that can no longer fail, so they are not optional here either. CI runs
# them from a multi-line block, which the lint exempts from matching, so this
# one is kept honest by the annotation rather than by the command text.
harness_self_tests() {
  local f
  for f in testing/test_*.py; do
    uv run --project testing python "$f" >/dev/null || {
      echo "$f failed; run it directly for the report"
      return 1
    }
  done
}
# mirrors: ha-ci.yml:lint-test-harness
gate "harness self-tests" harness_self_tests

if [ "$QUICK" -eq 0 ]; then
  # mirrors: ci.yml:test
  gate "cargo test" cargo test --workspace --verbose --locked
  # mirrors: ci.yml:doc
  gate "cargo doc" env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
fi

echo
if [ ${#failed[@]} -eq 0 ]; then
  echo "preflight: $passed gates passed"
  exit 0
fi
echo "preflight: ${#failed[@]} gate(s) failed — ${failed[*]}"
exit 1
