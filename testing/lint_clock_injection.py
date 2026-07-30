#!/usr/bin/env -S uv run --python 3.14 --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "rich>=14.0",
# ]
# ///
"""Fail if a safety decision reads the clock directly.

Every time comparison that gates write authority must run on one injectable
clock, or a `ManualClock` cannot drive the state machine and the lease, the
promotion hold-down, and the async-fallback grace can disagree with each other
under test while agreeing in production by accident.

The rule is scoped, not global. Most `Instant::now()` calls in this repo time an
HTTP request or stamp a log line and are none of this lint's business. What it
guards is the set of modules where a clock read decides whether this node may
accept writes:

    src/governor/lease.rs               lease expiry
    src/governor/raft.rs                metrics watchdog fence, leaderless recovery
    src/governor/replication_manager.rs async fallback grace
    src/app.rs                          promotion hold-down

`src/governor/state_machine.rs` is deliberately absent. LSN staleness rides in
replicated Raft state and snapshots, where a monotonic `Instant` is meaningless
across processes, so it uses the wall clock on purpose — documented in
HARDENING.md's Accepted risks and in docs/STATE_MACHINE.md.

Test code is exempt: a test constructing a real `Instant` as a fixture is not a
safety decision, and `ManualClock` itself has to build one somewhere.

Exit codes:
    0: no direct clock reads in guarded modules.
    1: at least one found.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Final

from rich.console import Console

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

GUARDED: Final[tuple[str, ...]] = (
    "src/governor/lease.rs",
    "src/governor/raft.rs",
    "src/governor/replication_manager.rs",
    "src/app.rs",
)

CLOCK_READ: Final[re.Pattern[str]] = re.compile(r"\b(?:Instant|SystemTime)::now\(\)")

# `Instant::elapsed()` reads the clock too, and reads it from the *wrong* one:
# it is implicitly `Instant::now() - self`, so it cannot be driven by a
# ManualClock even when the anchor was stamped from one.
ELAPSED_READ: Final[re.Pattern[str]] = re.compile(r"\.elapsed\(\)")

ALLOWED_CLOCK_SOURCE: Final[str] = "lease.read().now()"

ALLOW_MARKER: Final[str] = "clock-lint: allow"
"""Escape hatch, deliberately inline rather than a file list.

A guarded module still contains reads that decide nothing about write
authority — a rate-limiter window, a duration recorded for a histogram. Those
are fine, but the reason has to live at the line so it is reviewed with the
code that depends on it rather than drifting in a registry nobody opens."""

console = Console()


def guarded_violations(source: str) -> list[tuple[int, str]]:
    """Line numbers and text of direct clock reads outside `mod tests`.

    Everything from the first `mod tests` declaration to end of file is treated
    as test code. Crude, and correct for this repo: tests live in one trailing
    module per file.
    """
    lines = source.splitlines()
    cutoff = len(lines)
    for n, line in enumerate(lines):
        if re.match(r"\s*mod tests\b", line) or line.strip() == "#[cfg(test)]":
            cutoff = n
            break

    found: list[tuple[int, str]] = []
    for n, line in enumerate(lines[:cutoff], start=1):
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("///"):
            continue
        # The marker is idiomatically a comment above the line, not a trailing
        # one; accept either, and allow a wrapped two-line reason.
        window = lines[max(0, n - 3) : n]
        if any(ALLOW_MARKER in prior for prior in window):
            continue
        if CLOCK_READ.search(line) or ELAPSED_READ.search(line):
            found.append((n, stripped))
    return found


def main() -> int:
    problems: list[str] = []
    scanned = 0
    for rel in GUARDED:
        path = REPO_ROOT / rel
        if not path.exists():
            problems.append(f"{rel}: guarded module not found; the scan would pass vacuously")
            continue
        scanned += 1
        for line_no, text in guarded_violations(path.read_text(encoding="utf-8")):
            problems.append(f"{rel}:{line_no}: {text}")

    if scanned != len(GUARDED):
        console.print("[red]FAIL[/] guarded module list is stale")

    if problems:
        console.print(
            f"[red]FAIL[/] {len(problems)} direct clock read(s) in safety modules. "
            f"Read the injected clock instead — `{ALLOWED_CLOCK_SOURCE}` — so a "
            "ManualClock can drive these decisions:"
        )
        for problem in problems:
            console.print(f"  {problem}")
        return 1

    console.print(f"[green]PASS[/] no direct clock reads in {scanned} safety modules")
    return 0


if __name__ == "__main__":
    sys.exit(main())
