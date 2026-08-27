#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the continuous writers in the Track A/B upgrade harnesses.

The writers are `threading.Thread` subclasses. An instance attribute whose
name `Thread` uses internally replaces that internal for the object: binding
`_stop` to an Event makes `join()` raise `TypeError: 'Event' object is not
callable` at teardown, after every assertion in the proof has already passed.

`Thread._stop` is a method on CPython 3.12 and gone on 3.13, so the crash
reproduces on CI's interpreter and not on a newer local one. The forbidden
names are therefore listed rather than read off the running interpreter, and
only the attributes the subclass itself binds are checked.

Run with:
    uv run --project testing python testing/test_upgrade_writers.py
"""

from __future__ import annotations

import os
import sys
import threading
import unittest
from pathlib import Path
from typing import Final

sys.path.insert(0, str(Path(__file__).resolve().parent))

os.environ.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "self-test-token")

import pg_major_upgrade_proof
import pg_upgrade_hold_chaos

# Names `threading.Thread` has relied on across the versions this repo
# supports (3.10 through 3.13). Rebinding any of them breaks start/join.
THREAD_INTERNALS: Final[frozenset[str]] = frozenset(
    {
        "_bootstrap",
        "_delete",
        "_handle",
        "_is_stopped",
        "_reset_internal_locks",
        "_set_ident",
        "_set_tstate_lock",
        "_started",
        "_stop",
        "_tstate_lock",
        "_wait_for_tstate_lock",
    }
)


def _subclass_attributes(writer: threading.Thread) -> set[str]:
    """Attributes the subclass bound, excluding `Thread.__init__`'s own."""
    baseline = set(vars(threading.Thread(target=lambda: None)))
    return set(vars(writer)) - baseline


class WriterThreadHygieneTest(unittest.TestCase):
    """A writer must not take a name `threading.Thread` relies on."""

    def _assert_no_collision(self, writer: threading.Thread) -> None:
        collisions = sorted(_subclass_attributes(writer) & THREAD_INTERNALS)
        self.assertEqual(
            collisions,
            [],
            f"{type(writer).__name__} rebinds Thread internals {collisions}; "
            "join() uses these and fails at teardown on CPython 3.12",
        )

    def test_the_hold_chaos_writer_leaves_thread_internals_alone(self) -> None:
        self._assert_no_collision(pg_upgrade_hold_chaos.Writer(gateway_port=1))

    def test_the_major_upgrade_writer_leaves_thread_internals_alone(self) -> None:
        self._assert_no_collision(pg_major_upgrade_proof.Writer(gateway_port=1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
