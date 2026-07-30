#!/usr/bin/env -S uv run --project testing python
"""Proof that the clock-injection lint can fail.

`lint_clock_injection.py` is the only thing standing between the codebase and a
safety decision that reads the wall clock directly, and it has printed PASS
since the day it was written. A lint that has only ever passed is
indistinguishable from one that cannot fail — the same argument the contract
inversions rest on, applied to the lint itself.

Two things need proving. First that the scan catches each shape it claims to:
`Instant::now()`, `SystemTime::now()`, and `.elapsed()`, which is the one that
actually got past the first pass of H-30 because it is implicitly
`Instant::now() - self` and does not name a clock at all. Second that the
`mod tests` cutoff cannot silently swallow the file: the scan treats everything
after the boundary as test code, so a second `#[cfg(test)]` earlier in the file
would un-guard the lines between them while the lint went on reporting PASS.

Source snippets are literals, so this needs no cargo and no repo state.

Run with:
    uv run --project testing python testing/test_lint_clock_injection.py
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Final

from lint_clock_injection import GUARDED, AmbiguousTestBoundary, guarded_violations

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent


def lines_of(source: str) -> list[str]:
    """Violation text only, for readable assertions."""
    return [text for _, text in guarded_violations(source)]


class CatchesEveryClockShape(unittest.TestCase):
    """Each of these compiles, does something a `ManualClock` cannot drive, and
    must be reported."""

    def test_instant_now_is_a_violation(self) -> None:
        self.assertEqual(
            lines_of("let t = Instant::now();"),
            ["let t = Instant::now();"],
        )

    def test_system_time_now_is_a_violation(self) -> None:
        self.assertTrue(lines_of("let t = SystemTime::now();"))

    def test_elapsed_is_a_violation(self) -> None:
        """The shape that survived the first pass of H-30. It reads the process
        clock without naming it, so converting every `Instant::now()` looked
        like a complete migration while five gates still could not be driven."""
        self.assertTrue(lines_of("if self.anchor.elapsed() < timeout {"))

    def test_the_injected_clock_is_not_a_violation(self) -> None:
        """Otherwise the lint would forbid the very thing it demands."""
        self.assertEqual(lines_of("let now = self.lease.read().now();"), [])

    def test_a_comment_about_a_clock_read_is_not_a_violation(self) -> None:
        self.assertEqual(lines_of("// Anchored at Instant::now() by the governor."), [])
        self.assertEqual(lines_of("/// Stamped from `Instant::now()`."), [])


class AllowMarkerIsScoped(unittest.TestCase):
    """The escape hatch has to work, and has to stop working one line later."""

    def test_marker_on_the_line_above_allows_it(self) -> None:
        source = "// clock-lint: allow — histogram sample\nlet t = Instant::now();"
        self.assertEqual(lines_of(source), [])

    def test_marker_two_lines_above_still_allows_it(self) -> None:
        """A wrapped reason is two comment lines."""
        source = (
            "// clock-lint: allow — brute-force rate-limit window, decides\n"
            "// nothing about write authority.\n"
            "let t = Instant::now();"
        )
        self.assertEqual(lines_of(source), [])

    def test_marker_does_not_reach_a_later_read(self) -> None:
        """A marker that covered the rest of the file would let one justified
        exemption silently license every read after it."""
        source = (
            "// clock-lint: allow — histogram sample\n"
            "let sample = Instant::now();\n"
            "let a = 1;\n"
            "let b = 2;\n"
            "let gate = Instant::now();"
        )
        self.assertEqual(lines_of(source), ["let gate = Instant::now();"])


class TestCutoffCannotSwallowTheFile(unittest.TestCase):
    """The cutoff is the lint's own silent-failure mode."""

    def test_reads_after_the_test_module_are_ignored(self) -> None:
        source = "#[cfg(test)]\nmod tests {\n    let t = Instant::now();\n}"
        self.assertEqual(lines_of(source), [])

    def test_reads_before_the_test_module_are_caught(self) -> None:
        """Without this, the case above would also pass if the scan had stopped
        working entirely."""
        source = "let gate = Instant::now();\n#[cfg(test)]\nmod tests {\n}"
        self.assertEqual(lines_of(source), ["let gate = Instant::now();"])

    def test_a_second_boundary_is_a_loud_failure(self) -> None:
        """A test-only helper above the trailing module would move the cutoff up
        and un-guard everything after it. The lint must refuse rather than
        report PASS over the unscanned remainder."""
        source = (
            "#[cfg(test)]\n"
            "fn fixture() -> u8 { 0 }\n"
            "let gate = Instant::now();\n"
            "#[cfg(test)]\n"
            "mod tests {\n"
            "}"
        )
        with self.assertRaises(AmbiguousTestBoundary) as raised:
            guarded_violations(source)
        self.assertIn("1", str(raised.exception))
        self.assertIn("4", str(raised.exception))

    def test_the_attribute_and_its_module_are_one_boundary(self) -> None:
        """`#[cfg(test)]` and `mod tests {` are one boundary however far apart
        the attributes between them push the module — which in this repo is
        four lines of `#[allow(...)]`. Reading them as two was the first
        version of this check, and it failed on every guarded module."""
        source = (
            "let a = 1;\n"
            "#[cfg(test)]\n"
            "#[allow(\n"
            "    clippy::unwrap_used,\n"
            '    reason = "panics are the failure signal"\n'
            ")]\n"
            "mod tests {\n"
            "    let t = Instant::now();\n"
            "}"
        )
        self.assertEqual(lines_of(source), [])


class GuardedModulesExist(unittest.TestCase):
    """The lint reports the count it scanned, so a renamed module would be
    caught there too — but only once someone reads the output. Fail here
    instead."""

    def test_every_guarded_module_is_on_disk(self) -> None:
        for rel in GUARDED:
            with self.subTest(module=rel):
                self.assertTrue(
                    (REPO_ROOT / rel).exists(),
                    f"{rel} is guarded but does not exist; the scan would skip it",
                )

    def test_the_repo_currently_passes_its_own_lint(self) -> None:
        """Ties these cases to the real files: if the lint became unable to
        parse them, the synthetic snippets above would keep passing alone."""
        for rel in GUARDED:
            with self.subTest(module=rel):
                found = guarded_violations((REPO_ROOT / rel).read_text(encoding="utf-8"))
                self.assertEqual(found, [], f"{rel} has direct clock reads: {found}")


if __name__ == "__main__":
    unittest.main()
