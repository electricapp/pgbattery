#!/usr/bin/env -S uv run --project testing python
"""Unit tests for elle_adapter.

These tests do NOT require the Elle uberjar. They exercise the subprocess
driver's error paths and the result-parsing logic. Stdlib `unittest`, so
the test harness pyproject doesn't grow a pytest dependency.

The adapter is intentionally dumb: it does not convert formats. All the
worker-side encoding lives in linearizability_register.py and is exercised
by the real matrix runs.

Run with:
    uv run --project testing python testing/test_elle_adapter.py
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from elle_adapter import (
    ElleError,
    ElleOutput,
    check_with_elle,
)


def _parse(raw: dict[str, object]) -> ElleOutput:
    return ElleOutput.model_validate(raw)


class ParseValidTests(unittest.TestCase):
    def test_valid_true(self) -> None:
        self.assertIs(_parse({"valid?": True}).valid, True)

    def test_valid_false(self) -> None:
        self.assertIs(_parse({"valid?": False}).valid, False)

    def test_valid_unknown(self) -> None:
        self.assertIsNone(_parse({"valid?": "unknown"}).valid)

    def test_valid_missing(self) -> None:
        self.assertIsNone(_parse({}).valid)


class ParseAnomaliesTests(unittest.TestCase):
    def test_no_anomalies(self) -> None:
        self.assertEqual(_parse({"anomalies": {}}).flat_anomalies, [])

    def test_missing_anomalies_key(self) -> None:
        self.assertEqual(_parse({}).flat_anomalies, [])

    def test_anomalies_with_cycles(self) -> None:
        raw: dict[str, object] = {
            "anomalies": {
                "G-single": [{"cycle": [3, 7, 9, 3]}],
                "G2-item": [{"cycle": [1, 2, 1]}, {"cycle": [4, 5, 4]}],
            }
        }
        anomalies = _parse(raw).flat_anomalies
        self.assertEqual(len(anomalies), 3)
        names = sorted(a.name for a in anomalies)
        self.assertEqual(names, ["G-single", "G2-item", "G2-item"])
        g_single = next(a for a in anomalies if a.name == "G-single")
        self.assertEqual(g_single.cycle, [3, 7, 9, 3])

    def test_steps_are_read_as_a_cycle(self) -> None:
        """Elle spells the same thing `steps` for some anomaly classes."""
        anomalies = _parse({"anomalies": {"G0": [{"steps": [2, 4, 2]}]}}).flat_anomalies
        self.assertEqual(anomalies[0].cycle, [2, 4, 2])

    def test_an_op_map_in_a_cycle_is_not_carried_as_an_index(self) -> None:
        """A whole op map is not an index, and printing one as an index would
        put a number in the report that identifies nothing."""
        anomalies = _parse(
            {"anomalies": {"G-single": [{"cycle": [3, {"process": 1}, 5]}]}}
        ).flat_anomalies
        self.assertEqual(anomalies[0].cycle, [3, 5])

    def test_scalar_anomaly_payload(self) -> None:
        anomalies = _parse({"anomalies": {"some-flag": True}}).flat_anomalies
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0].name, "some-flag")
        self.assertEqual(anomalies[0].detail["payload"], True)

    def test_meta_op_count_and_elapsed(self) -> None:
        parsed = _parse({"_meta": {"op-count": 4_200}, "elapsed-ms": 12})
        self.assertEqual(parsed.meta.op_count, 4_200)
        self.assertEqual(parsed.elapsed_ms, 12.0)


class SubprocessErrorTests(unittest.TestCase):
    def test_missing_jar(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            history = Path(td) / "h.json"
            history.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ElleError, "uberjar not found"):
                check_with_elle(history, jar_path=Path(td) / "missing.jar")

    def test_missing_history(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake_jar = Path(td) / "fake.jar"
            fake_jar.write_bytes(b"not a real jar")
            with self.assertRaisesRegex(ElleError, "History file does not exist"):
                check_with_elle(Path(td) / "absent.json", jar_path=fake_jar)


if __name__ == "__main__":
    unittest.main()
