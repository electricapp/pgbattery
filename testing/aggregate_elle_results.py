#!/usr/bin/env -S uv run --project testing python
"""Aggregate per-attack Elle results into a single Markdown summary table.

Reads testing/artifacts/elle-*/elle_result.json and testing/artifacts/elle-*/results.json,
writes testing/artifacts/elle-summary.md with one row per attack.

Exit codes:
  0 - every attack passed
  1 - at least one attack found an anomaly
  2 - at least one attack had infrastructure failure (missing artifacts)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ARTIFACT_ROOT = Path("testing/artifacts")
SUMMARY_OUT = ARTIFACT_ROOT / "elle-summary.md"


def _format_anomalies(summary: dict[str, int]) -> str:
    if not summary:
        return "-"
    return ", ".join(f"{name}x{count}" for name, count in sorted(summary.items()))


def _verdict_label(valid: object) -> str:
    if valid is True:
        return "PASS"
    if valid is False:
        return "FAIL"
    return "UNKNOWN"


def _fixed_width_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Pipe-table with each cell padded to its column's max width.

    Markdown renderers ignore the padding, but raw text views (terminal,
    less, `cat`) get aligned columns.
    """
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in rows)) if rows else len(headers[i])
        for i in range(len(headers))
    ]

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells)) + " |"

    lines = [fmt_row(headers)]
    lines.append("|" + "|".join("-" * (w + 2) for w in widths) + "|")
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def main() -> int:
    raw_rows: list[dict[str, str]] = []
    any_invalid = False
    any_missing = False

    attack_dirs = sorted(p for p in ARTIFACT_ROOT.glob("elle-*") if p.is_dir())
    if not attack_dirs:
        print(f"No elle-* artifact dirs under {ARTIFACT_ROOT}", file=sys.stderr)
        return 2

    for d in attack_dirs:
        attack = d.name.removeprefix("elle-")
        result_path = d / "elle_result.json"
        outer_path = d / "results.json"

        if not result_path.exists():
            raw_rows.append(
                {
                    "attack": attack,
                    "ops": "-",
                    "valid": "ERROR",
                    "anomalies": "missing elle_result.json",
                    "elle_ms": "-",
                }
            )
            any_missing = True
            continue

        elle = json.loads(result_path.read_text())
        outer = json.loads(outer_path.read_text()) if outer_path.exists() else {}

        valid = elle.get("valid")
        if valid is not True:
            any_invalid = True

        raw_rows.append(
            {
                "attack": attack,
                "ops": str(elle.get("op_count", outer.get("workers", "-"))),
                "valid": _verdict_label(valid),
                "anomalies": _format_anomalies(elle.get("anomaly_summary", {})),
                "elle_ms": f"{elle.get('elapsed_ms', 0):.0f}",
            }
        )

    headers = ["Attack", "Ops", "Valid", "Anomalies", "Elle ms"]
    rows = [[r["attack"], r["ops"], r["valid"], r["anomalies"], r["elle_ms"]] for r in raw_rows]

    lines = [
        "# Elle x Attack Matrix",
        "",
        "Per-attack consistency check results from `testing/run_elle_matrix.sh`.",
        "Strict-serializable model via Elle v0.2.2.",
        "",
    ]
    lines.extend(_fixed_width_table(headers, rows))
    lines.append("")
    lines.append(
        "Legend: PASS = strict-serializable; FAIL = Elle found anomaly cycles; "
        "UNKNOWN = Elle returned indeterminate; ERROR = infrastructure failure."
    )
    lines.append("")

    SUMMARY_OUT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWrote {SUMMARY_OUT}")

    if any_missing:
        return 2
    if any_invalid:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
