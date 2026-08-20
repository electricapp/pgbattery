#!/usr/bin/env -S uv run --project testing python
"""Doubles the harness self-tests drive their subjects with.

Every fault primitive reaches the world through two seams — a shell runner and
a database connector — and both can be swapped. Tests that use the seams
exercise the real code under a scripted world; tests that reach for
``mock.patch`` on an internal function instead end up asserting the call graph,
so they break whenever the code is refactored and pass whenever the wiring is
wrong in a way the mock happens to paper over.

These live here rather than in one test module because two of them need the
same doubles, and a fake copied is a fake that drifts.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable, Sequence
from typing import Any
from unittest import mock

import psycopg

from fault_primitives import CommandResult, set_command_runner, set_event_sink


class ScriptedRunner:
    """Answers shell commands by matching substrings, recording every call.

    Rules are ``(needle, CommandResult)`` pairs, checked in order; the first
    match wins. An unmatched command returns rc 0 with empty output, which is
    what a successful ``tc``/``kill``/``rm`` looks like.
    """

    def __init__(self, rules: Sequence[tuple[str, CommandResult]] = ()) -> None:
        self.rules = list(rules)
        self.calls: list[str] = []

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        self.calls.append(cmd)
        for needle, result in self.rules:
            if needle in cmd:
                return result
        return CommandResult(0, "", "")

    def matching(self, needle: str) -> list[str]:
        return [call for call in self.calls if needle in call]


def ok(stdout: str = "") -> CommandResult:
    return CommandResult(0, stdout, "")


def fail(stderr: str, rc: int = 1) -> CommandResult:
    return CommandResult(rc, "", stderr)


class ScriptedCursor:
    """A cursor that records statements and answers from a scripted table.

    ``rows`` is what the next ``SELECT`` returns. ``raises`` maps a substring
    of a statement to the error executing it should raise, which is how a
    fenced primary, a duplicate key and a dropped connection are expressed
    without a database.
    """

    def __init__(
        self,
        rows: Sequence[tuple[Any, ...]],
        raises: dict[str, Exception],
        heals_after: int | None = None,
    ) -> None:
        self.rows = list(rows)
        self.raises = raises
        self.heals_after = heals_after
        self.refusals = 0
        self.statements: list[str] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.statements.append(sql)
        for needle, error in self.raises.items():
            if needle not in sql:
                continue
            if self.heals_after is not None and self.refusals >= self.heals_after:
                return
            self.refusals += 1
            raise error

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def __enter__(self) -> ScriptedCursor:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class ScriptedConnection:
    """One connection, handing out a single shared cursor."""

    def __init__(self, cursor: ScriptedCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> ScriptedCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> ScriptedConnection:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class ScriptedSql:
    """A connector double: what every node's database does, scripted.

    ``connect_raises`` stands for a node that cannot be reached at all, which
    is a different finding from one that refuses a statement.
    """

    def __init__(
        self,
        *,
        rows: Sequence[tuple[Any, ...]] = (),
        raises: dict[str, Exception] | None = None,
        heals_after: int | None = None,
        connect_raises: Exception | None = None,
    ) -> None:
        self.cursor = ScriptedCursor(rows, raises or {}, heals_after)
        self.connect_raises = connect_raises
        self.nodes: list[str] = []

    def __call__(self, node: str) -> ScriptedConnection:
        self.nodes.append(node)
        if self.connect_raises is not None:
            raise self.connect_raises
        return ScriptedConnection(self.cursor)

    @property
    def statements(self) -> list[str]:
        return self.cursor.statements

    def issued(self, needle: str) -> list[str]:
        return [sql for sql in self.statements if needle in sql]


def read_only() -> psycopg.Error:
    """What a fenced primary raises for any write."""
    return psycopg.errors.ReadOnlySqlTransaction("cannot execute in a read-only transaction")


def duplicate_key() -> psycopg.Error:
    """What a second run writing the first run's keys raises."""
    return psycopg.errors.UniqueViolation("duplicate key value violates unique constraint")


class VirtualClock:
    """A clock a `sleep` moves forward instead of waiting on.

    Advances by at least `TICK` even for a zero-length sleep, so a poll loop
    that does not sleep still reaches its deadline rather than spinning.
    """

    TICK = 0.01

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(seconds, self.TICK)


class HarnessFixture(unittest.TestCase):
    """Installs scripted seams for the test and restores them afterwards."""

    def install(self, runner: ScriptedRunner) -> ScriptedRunner:
        previous_runner = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous_runner)
        self.events: list[dict[str, object]] = []
        previous_sink = set_event_sink(self.events.append)
        self.addCleanup(set_event_sink, previous_sink)
        return runner

    def install_sql(
        self, connector: Callable[[str], Any], setter: Callable[[Any], Any]
    ) -> Callable[[str], Any]:
        """Swap a module's database connector through its own setter."""
        previous = setter(connector)
        self.addCleanup(setter, previous)
        return connector

    def no_waiting(self) -> VirtualClock:
        """Run poll loops on a clock that a `sleep` advances rather than waits.

        Every wait here is bounded by a deadline read from `time.monotonic`, so
        a test driving one to its timeout would otherwise spend that timeout —
        and merely disabling `sleep` makes it spin for the same duration, which
        is worse. Advancing the clock instead exercises the same deadline
        arithmetic in no time at all.
        """
        clock = VirtualClock()
        for patcher in (
            mock.patch("time.monotonic", clock.monotonic),
            mock.patch("time.sleep", clock.sleep),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        return clock
