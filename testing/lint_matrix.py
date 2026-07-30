#!/usr/bin/env -S uv run --python 3.14 --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "rich>=14.0",
#     "typer>=0.21",
# ]
# ///
"""Lint the CI test harness: matrix structure, Python syntax, SQL and contract refs.

Runs fast (~100ms) and catches broken matrix/runner/SQL combinations before
they hit the heavy integration tests.

Checks:
    1. ``ci_matrix.yaml`` is valid JSON with expected top-level keys.
    2. All Python test scripts (``ci_runner.py``, ``correctness_lite.py``,
       ``overnight_test.py``, and the optional scripts that exist) parse without
       syntax errors.
    3. Every ``sql`` step in the matrix references a file that exists in
       ``testing/sql/``, and every ``.sql`` file on disk is referenced.
    4. Every step ``type`` used in the matrix is defined in the ``StepType``
       enum in ``ci_runner.py``.
    5. Every case ID referenced by a suite exists in the ``cases`` list.
    6. All ``.sql`` files are non-empty valid UTF-8.
    7. No duplicate case IDs in the matrix.
    8. ``docs/CONTRACTS.md`` still defines parseable contract IDs.
    9. Every case declares at least one contract ID, and every declared ID is
       defined in ``docs/CONTRACTS.md`` — the policy that doc states but that
       nothing enforced while ``contracts`` was an undeclared field.

Exit codes:
    0: All checks passed.
    1: One or more checks failed.
"""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

TESTING_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTING_DIR.parent
SQL_DIR = TESTING_DIR / "sql"
MATRIX_PATH = TESTING_DIR / "ci_matrix.yaml"
RUNNER_PATH = TESTING_DIR / "ci_runner.py"
CORRECTNESS_LITE_PATH = TESTING_DIR / "correctness_lite.py"
OVERNIGHT_PATH = TESTING_DIR / "overnight_test.py"
LINEARIZABILITY_PATH = TESTING_DIR / "linearizability_register.py"
UNIT_TEST_PATH = TESTING_DIR / "test_ci_runner_units.py"
CONTRACTS_PATH = PROJECT_ROOT / "docs" / "CONTRACTS.md"

# Optional scripts are checked only if they exist (skeletons during build-out).
_OPTIONAL_SCRIPTS = [LINEARIZABILITY_PATH, UNIT_TEST_PATH]
PYTHON_SCRIPTS = [RUNNER_PATH, CORRECTNESS_LITE_PATH, OVERNIGHT_PATH] + [
    p for p in _OPTIONAL_SCRIPTS if p.exists()
]

# Contract IDs are the `### <ID> — <title>` headings in docs/CONTRACTS.md.
CONTRACT_HEADING_PATTERN = re.compile(r"^###\s+([A-Z]{1,3}[0-9]{1,2})\b", re.MULTILINE)
# Cases named in a violation message before it switches to a count.
_MAX_LISTED_CASES = 12

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
# Contract-to-test policy
# ---------------------------------------------------------------------------


def extract_contract_ids(markdown: str) -> set[str]:
    """Collect the contract IDs defined by ``docs/CONTRACTS.md``.

    A contract is defined by an ``### <ID> — <title>`` heading; the
    contract-to-test index table at the bottom of the doc is derived from those
    headings and is deliberately not treated as a definition site.

    Args:
        markdown: Contents of ``docs/CONTRACTS.md``.

    Returns:
        Set of contract IDs (e.g. ``{"W1", "L2", "R1"}``).
    """
    return set(CONTRACT_HEADING_PATTERN.findall(markdown))


def _format_case_list(case_ids: list[str]) -> str:
    """Render a case-ID list, truncating long lists to keep messages readable."""
    if len(case_ids) <= _MAX_LISTED_CASES:
        return ", ".join(case_ids)
    shown = ", ".join(case_ids[:_MAX_LISTED_CASES])
    return f"{shown}, ... (+{len(case_ids) - _MAX_LISTED_CASES} more)"


def collect_contract_violations(cases: list[dict[str, Any]], known_ids: set[str]) -> list[str]:
    """Check every case against the contract-reference policy.

    Policy (stated by ``docs/CONTRACTS.md``): every CI test case must reference
    at least one contract ID, and every referenced ID must be a contract the doc
    actually defines.

    Args:
        cases: Raw case dicts from the matrix.
        known_ids: Contract IDs defined in ``docs/CONTRACTS.md``.

    Returns:
        Actionable violation messages (empty when the policy holds).
    """
    missing: list[str] = []
    malformed: list[str] = []
    unknown: list[str] = []
    for case in cases:
        case_id = str(case.get("id", "<unnamed>"))
        declared = case.get("contracts")
        if declared is None or declared == []:
            missing.append(case_id)
            continue
        if not isinstance(declared, list) or not all(isinstance(item, str) for item in declared):
            malformed.append(f"{case_id} (contracts={declared!r})")
            continue
        unresolved = sorted(set(declared) - known_ids)
        if unresolved:
            unknown.append(f"{case_id} -> {unresolved}")

    violations: list[str] = []
    if missing:
        violations.append(
            f"{len(missing)} case(s) declare no contracts. Add "
            f'"contracts": ["W1", ...] to each case in testing/ci_matrix.yaml '
            f"(docs/CONTRACTS.md: every CI test case must reference at least one "
            f"contract ID). Cases: {_format_case_list(missing)}"
        )
    if malformed:
        violations.append(
            f"{len(malformed)} case(s) have a non-list-of-strings 'contracts' field: "
            f"{_format_case_list(malformed)}"
        )
    if unknown:
        violations.append(
            f"{len(unknown)} case(s) reference contract IDs that docs/CONTRACTS.md does not "
            f"define: {_format_case_list(unknown)}. Defined IDs: {sorted(known_ids)}"
        )
    return violations


def check_contracts_doc() -> None:
    """Verify docs/CONTRACTS.md exists and still yields parseable contract IDs."""
    if not CONTRACTS_PATH.exists():
        raise AssertionError(f"{CONTRACTS_PATH} not found")
    ids = extract_contract_ids(CONTRACTS_PATH.read_text(encoding="utf-8"))
    if not ids:
        raise AssertionError(
            f"no '### <ID> — <title>' contract headings found in {CONTRACTS_PATH}; "
            f"the contract-reference lint cannot run against an empty contract set"
        )


def check_case_contract_refs() -> None:
    """Enforce the contract-reference policy on every case in the matrix."""
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    known_ids = extract_contract_ids(CONTRACTS_PATH.read_text(encoding="utf-8"))
    violations = collect_contract_violations(data["cases"], known_ids)
    if violations:
        raise AssertionError(" | ".join(violations))


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
    check("CONTRACTS.md defines contract IDs", check_contracts_doc)
    check("Cases reference real contract IDs", check_case_contract_refs)

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
