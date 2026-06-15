#!/usr/bin/env -S uv run --python 3.14 --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "rich>=14.0",
#     "typer>=0.21",
# ]
# ///
"""Lint the CI test harness: validate JSON, Python syntax, SQL file references.

Runs fast (~100ms) and catches broken matrix/runner/SQL combinations before
they hit the heavy integration tests.

Checks:
    1. ``ci_matrix.yaml`` is valid JSON with expected top-level keys.
    2. All Python test scripts (``ci_runner.py``, ``correctness_lite.py``,
       ``overnight_test.py``) parse without syntax errors.
    3. Every ``sql`` step in the matrix references a file that exists in
       ``testing/sql/``, and every ``.sql`` file on disk is referenced.
    4. Every step ``type`` used in the matrix is defined in the ``StepType``
       enum in ``ci_runner.py``.
    5. Every case ID referenced by a suite exists in the ``cases`` list.
    6. All ``.sql`` files are non-empty valid UTF-8.
    7. No duplicate case IDs in the matrix.

Exit codes:
    0: All checks passed.
    1: One or more checks failed.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Callable
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

TESTING_DIR = Path(__file__).resolve().parent
SQL_DIR = TESTING_DIR / "sql"
MATRIX_PATH = TESTING_DIR / "ci_matrix.yaml"
RUNNER_PATH = TESTING_DIR / "ci_runner.py"
CORRECTNESS_LITE_PATH = TESTING_DIR / "correctness_lite.py"
OVERNIGHT_PATH = TESTING_DIR / "overnight_test.py"
LINEARIZABILITY_PATH = TESTING_DIR / "linearizability_register.py"

# Optional scripts are checked only if they exist (skeletons during build-out).
_OPTIONAL_SCRIPTS = [LINEARIZABILITY_PATH]
PYTHON_SCRIPTS = [RUNNER_PATH, CORRECTNESS_LITE_PATH, OVERNIGHT_PATH] + [
    p for p in _OPTIONAL_SCRIPTS if p.exists()
]

console = Console()
results: list[tuple[str, bool, str]] = []


def check(name: str, fn: Callable[[], None]) -> None:
    """Run a check function and record the outcome.

    Args:
        name: Human-readable check name for the results table.
        fn: Callable that raises on failure.
    """
    try:
        fn()
        results.append((name, True, ""))
    except Exception as exc:
        results.append((name, False, str(exc)))


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_matrix_json() -> None:
    """Validate ci_matrix.yaml is well-formed JSON with required keys."""
    text = MATRIX_PATH.read_text(encoding="utf-8")
    data = json.loads(text)
    assert isinstance(data, dict), "top-level value must be an object"
    assert "cases" in data, "missing 'cases' key"
    assert "suites" in data, "missing 'suites' key"


def check_python_syntax() -> None:
    """Validate all Python test scripts parse without syntax errors."""
    for script in PYTHON_SCRIPTS:
        if not script.exists():
            raise AssertionError(f"{script.name} not found")
        source = script.read_text(encoding="utf-8")
        ast.parse(source, filename=str(script))


def check_sql_references() -> None:
    """Verify every SQL file referenced by the matrix exists, and vice versa."""
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    referenced: set[str] = set()
    for case in data["cases"]:
        for phase in ["actions", "assertions", "cleanup"]:
            for step in case.get(phase, []):
                if step.get("type") == "sql":
                    referenced.add(step["file"])

    on_disk = {f.name for f in SQL_DIR.iterdir() if f.suffix == ".sql"}

    missing = referenced - on_disk
    orphaned = on_disk - referenced

    msgs: list[str] = []
    if missing:
        msgs.append(f"referenced but missing on disk: {sorted(missing)}")
    if orphaned:
        msgs.append(f"on disk but not referenced by matrix: {sorted(orphaned)}")
    if msgs:
        raise AssertionError("; ".join(msgs))


def check_step_types() -> None:
    """Verify every step type used in the matrix is defined in StepType enum."""
    runner_source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(runner_source)

    defined_types: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "StepType":
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and isinstance(item.value, ast.Constant):
                            value = item.value.value
                            if isinstance(value, str):
                                defined_types.add(value)

    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    used_types: set[str] = set()
    for case in data["cases"]:
        for phase in ["actions", "assertions", "cleanup"]:
            for step in case.get(phase, []):
                used_types.add(step["type"])

    unknown = used_types - defined_types
    if unknown:
        raise AssertionError(f"step types used in matrix but not in StepType: {sorted(unknown)}")


def check_suite_case_refs() -> None:
    """Verify every case ID referenced by a suite exists in the cases list."""
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    case_ids = {case["id"] for case in data["cases"]}
    for suite_name, suite in data["suites"].items():
        for case_id in suite["cases"]:
            if case_id not in case_ids:
                raise AssertionError(f"suite '{suite_name}' references unknown case '{case_id}'")


def check_sql_files_valid() -> None:
    """Basic validation that SQL files are non-empty and UTF-8 decodable."""
    for sql_file in sorted(SQL_DIR.iterdir()):
        if sql_file.suffix != ".sql":
            continue
        content = sql_file.read_text(encoding="utf-8")
        if not content.strip():
            raise AssertionError(f"{sql_file.name} is empty")


def check_no_duplicate_case_ids() -> None:
    """Verify no duplicate case IDs in the matrix."""
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    ids = [case["id"] for case in data["cases"]]
    seen: set[str] = set()
    for case_id in ids:
        if case_id in seen:
            raise AssertionError(f"duplicate case ID: '{case_id}'")
        seen.add(case_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    add_completion=False,
    help="Lint the pgbattery CI test harness (matrix, SQL, Python scripts).",
)


@app.command()
def lint() -> None:
    """Run all lint checks and print a summary table."""
    check("ci_matrix.yaml is valid JSON", check_matrix_json)
    check("Python scripts parse cleanly", check_python_syntax)
    check("SQL file references match disk", check_sql_references)
    check("All step types are defined", check_step_types)
    check("Suite case refs exist", check_suite_case_refs)
    check("SQL files are non-empty UTF-8", check_sql_files_valid)
    check("No duplicate case IDs", check_no_duplicate_case_ids)

    table = Table(title="Test Harness Lint", show_lines=False)
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")

    failures = 0
    for name, passed, detail in results:
        status = "[green]PASS[/]" if passed else "[red]FAIL[/]"
        table.add_row(name, status, detail if not passed else "")
        if not passed:
            failures += 1

    console.print(table)
    console.print()

    if failures:
        console.print(f"[red bold]{failures} check(s) failed[/]")
        raise typer.Exit(code=1)
    else:
        console.print(f"[green bold]All {len(results)} checks passed[/]")
        raise typer.Exit(code=0)


if __name__ == "__main__":
    app()
