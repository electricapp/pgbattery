#!/usr/bin/env -S uv run --python 3.14 --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "rich>=14.0",
# ]
# ///
"""Fail if anything but `governor::propose` writes to the Raft log.

Every command must be admitted against the committed feature level before it
is proposed. A proposal that skips the check can commit an entry a member still
running the previous binary cannot decode, and that member's replication stalls
at it for good. The state machine's own refusal does not help: an entry the old
binary cannot parse never reaches its apply.

So the rule is structural, not advisory. `client_write` appears in
`src/governor/propose.rs` and nowhere else in the Rust tree, comments aside.

Exit codes:
    0: every proposal goes through `governor::propose`.
    1: a `client_write` call outside it.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Final

from rich.console import Console

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

DOOR: Final[str] = "src/governor/propose.rs"

SCANNED_ROOTS: Final[tuple[str, ...]] = ("src", "crates")

PROPOSAL: Final[re.Pattern[str]] = re.compile(r"\.client_write\s*\(")

console = Console()


def proposals_outside_the_door(source: str) -> list[tuple[int, str]]:
    """Line numbers and text of `client_write` calls, comments excluded."""
    found: list[tuple[int, str]] = []
    for n, line in enumerate(source.splitlines(), start=1):
        code = line.split("//", 1)[0]
        if PROPOSAL.search(code):
            found.append((n, line.strip()))
    return found


def main() -> int:
    door = REPO_ROOT / DOOR
    if not door.exists():
        console.print(f"[red]FAIL[/] {DOOR} not found; the scan would pass vacuously")
        return 1

    problems: list[str] = []
    scanned = 0
    for root in SCANNED_ROOTS:
        for path in sorted((REPO_ROOT / root).rglob("*.rs")):
            rel = path.relative_to(REPO_ROOT).as_posix()
            if rel == DOOR:
                continue
            scanned += 1
            for line_no, text in proposals_outside_the_door(path.read_text(encoding="utf-8")):
                problems.append(f"{rel}:{line_no}: {text}")

    if scanned == 0:
        console.print("[red]FAIL[/] scanned no Rust sources; the scan would pass vacuously")
        return 1

    if problems:
        console.print(
            f"[red]FAIL[/] {len(problems)} Raft proposal(s) bypass the feature-level "
            "admission. Call `governor::propose::propose` instead:"
        )
        for problem in problems:
            console.print(f"  {problem}")
        return 1

    console.print(f"[green]PASS[/] {scanned} Rust files propose only through {DOOR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
