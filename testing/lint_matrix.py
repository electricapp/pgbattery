#!/usr/bin/env -S uv run --python 3.14 --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "rich>=14.0",
#     "typer>=0.21",
#     # topology.py reads docker-compose.yml, which is the truth source for the
#     # cluster the matrix claims to describe, and parses it into models.
#     "pyyaml>=6.0",
#     "pydantic>=2.12",
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
from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import typer
import yaml
from pydantic import BaseModel, ConfigDict
from rich.console import Console
from rich.table import Table

import topology

TESTING_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTING_DIR.parent
SQL_DIR = TESTING_DIR / "sql"
MATRIX_PATH = TESTING_DIR / "ci_matrix.yaml"
RUNNER_PATH = TESTING_DIR / "ci_runner.py"
CORRECTNESS_LITE_PATH = TESTING_DIR / "correctness_lite.py"
OVERNIGHT_PATH = TESTING_DIR / "overnight_test.py"
LINEARIZABILITY_PATH = TESTING_DIR / "linearizability_register.py"
ELLE_MATRIX_PATH = TESTING_DIR / "run_elle_matrix.sh"
UNIT_TEST_PATH = TESTING_DIR / "test_ci_runner_units.py"
CONTRACTS_PATH = PROJECT_ROOT / "docs" / "CONTRACTS.md"

DEFAULT_TRANSFER_RETRY_SEC = 60
"""`ci_runner.py`'s default `retry_sec` for a transfer_leadership step."""

MIN_TRANSFER_RETRY_SEC = 30
"""Floor for that budget: a demote stops PostgreSQL, may run pg_rewind, and
restarts into recovery, and the target refuses the transfer throughout."""

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


class MatrixEnvelope(BaseModel):
    """The two keys every other check in this file reads.

    Both are required: a matrix missing either would make every check over it
    pass over an empty list, which reads as a clean lint.
    """

    model_config = ConfigDict(extra="allow")

    cases: list[dict[str, Any]]
    suites: dict[str, dict[str, Any]]


def check_matrix_json() -> None:
    """Validate ci_matrix.yaml is well-formed JSON with required keys."""
    MatrixEnvelope.model_validate(json.loads(MATRIX_PATH.read_text(encoding="utf-8")))


def check_python_syntax() -> None:
    """Validate all Python test scripts parse without syntax errors."""
    for script in PYTHON_SCRIPTS:
        if not script.exists():
            raise AssertionError(f"{script.name} not found")
        source = script.read_text(encoding="utf-8")
        ast.parse(source, filename=str(script))


def check_sql_references() -> None:
    """Verify every SQL file referenced by the matrix exists, and vice versa.

    The matrix is not the only caller. Standalone harnesses — the ones that
    drive a scenario the matrix has no step type for — reference their SQL by
    filename too, and a file used by one of those is not orphaned. Both
    directions still hold: a matrix reference with no file fails, and a file no
    caller anywhere names fails.
    """
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    referenced: set[str] = set()
    for case in data["cases"]:
        for phase in ["actions", "assertions", "cleanup"]:
            for step in case.get(phase, []):
                if step.get("type") == "sql":
                    referenced.add(step["file"])

    on_disk = {f.name for f in SQL_DIR.iterdir() if f.suffix == ".sql"}

    # Anything a harness script names by filename counts as a reference.
    harness_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(TESTING_DIR.glob("*.py"))
        if path.name != "lint_matrix.py"
    )
    referenced_by_harness = {name for name in on_disk if name in harness_text}

    missing = referenced - on_disk
    orphaned = on_disk - referenced - referenced_by_harness

    msgs: list[str] = []
    if missing:
        msgs.append(f"referenced but missing on disk: {sorted(missing)}")
    if orphaned:
        msgs.append(f"on disk but named by neither the matrix nor a harness: {sorted(orphaned)}")
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


RAW_FAULT_VERB: Final[re.Pattern[str]] = re.compile(
    r"docker\s+(?:network\s+(?:dis)?connect|kill|stop|start|pause|unpause|restart)\b"
    r"|\biptables\s+-[AIDF]\b"
    r"|\btc\s+(?:qdisc|filter)\s+(?:add|del|change|replace)\b"
)
"""Shell verbs that mutate network state to inject or heal a fault.

Each entry requires the mutating subcommand, not just the tool name. A module
legitimately names ``iptables`` without running it — asserting the binary
exists in the image, labelling a log file — and flagging the bare word would
push callers into renaming things to get past the check rather than routing the
command through the primitive layer.

``docker`` is a tool whose ordinary uses far outnumber its fault ones, so only
its fault subcommands are listed: ``docker compose ps`` and ``docker exec psql``
are reads and must stay allowed.
"""

PRIMITIVE_MODULE: Final[str] = "fault_primitives.py"

PENDING_FAULT_MIGRATION: Final[frozenset[str]] = frozenset()
"""Modules that still inject faults directly, each tracked by a task.

Now empty: every harness routes through the primitive layer. Anything added
here is a regression that owes a task, not a standing exemption.

The point of this list is to stop the *spread*. It is not a correctness check:
matching source text cannot tell a command from a sentence about a command, and
a verb assembled at runtime is invisible to it. What it does buy is that a new
module cannot quietly start injecting faults, and that an entry cannot be
forgotten — a file that becomes clean has to be removed from the list, which is
the only ratchet worth having here.
"""

FAULT_VERB_SCAN_EXEMPT: Final[frozenset[str]] = frozenset(
    {
        PRIMITIVE_MODULE,
        # This module has to spell the verbs out to detect them.
        "lint_matrix.py",
    }
)


def count_raw_fault_verbs(source: str) -> list[int]:
    """Line numbers of string literals containing a raw fault verb.

    Parses rather than greps so comments and docstrings — which discuss these
    commands constantly — cannot register as injections. Docstrings are string
    literals too, so they are excluded explicitly.

    Deliberately shallow: a verb assembled at runtime is invisible to it. It
    answers "does this module name a fault command", which is enough to notice a
    new module starting to, and not enough to be relied on for anything else.
    """
    tree = ast.parse(source)
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))
    return sorted(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and RAW_FAULT_VERB.search(node.value)
    )


CONTRACT_INDEX_ROW: Final[re.Pattern[str]] = re.compile(r"^\|(?P<cells>.+)\|\s*$")


def parse_contract_index(markdown: str) -> list[list[str]]:
    """Rows of the Contract-to-Test Index, cells stripped.

    Header and separator rows are dropped, as is anything before the index
    heading, so prose tables elsewhere in the document cannot be mistaken for
    coverage declarations.
    """
    rows: list[list[str]] = []
    _, _, tail = markdown.partition("## Contract-to-Test Index")
    for line in tail.splitlines():
        match = CONTRACT_INDEX_ROW.match(line.strip())
        if match is None:
            if rows:
                break  # table ended
            continue
        cells = [cell.strip() for cell in match.group("cells").split("|")]
        if all(set(cell) <= {"-", ":"} for cell in cells if cell):
            continue  # separator
        rows.append(cells)
    return rows[1:] if rows else []


BACKTICKED: Final[re.Pattern[str]] = re.compile(r"`([^`]+)`")
"""Identifiers in the index are backticked; surrounding prose is not.

Only backticked tokens are resolved. Prose like "LSN-gate proptest" is a
pointer for a human and is not claimed to be a name, so demanding it resolve
would push authors into deleting the context instead of writing it.
"""

BRACE_GROUP: Final[re.Pattern[str]] = re.compile(r"^(.*)\{([^}]*)\}(.*)$")
"""`assert-sanity-chaos-oracle{,-post,-full}` names three cases."""

TEST_FN: Final[re.Pattern[str]] = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?(?:fn|def)\s+(\w+)")


def expand_braces(token: str) -> list[str]:
    """Expand one shell-style brace group, if present."""
    match = BRACE_GROUP.match(token)
    if match is None:
        return [token]
    prefix, alternatives, suffix = match.groups()
    return [f"{prefix}{alt}{suffix}" for alt in alternatives.split(",")]


def _known_test_function_names() -> set[str]:
    """Every Rust and Python function name in the tree.

    Broader than "test functions" on purpose: an inversion may point at a
    helper, and the question being asked is only whether the name still exists.
    """
    names: set[str] = set()
    roots = (PROJECT_ROOT / "src", PROJECT_ROOT / "crates", TESTING_DIR)
    for root in roots:
        for path in root.rglob("*"):
            if path.suffix not in {".rs", ".py"} or not path.is_file():
                continue
            if "target" in path.parts or "__pycache__" in path.parts:
                continue
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                found = TEST_FN.match(line)
                if found:
                    names.add(found.group(1))
    return names


def unresolved_inversion_refs(inversion: str, case_ids: set[str], fn_names: set[str]) -> list[str]:
    """Backticked names in an inversion cell that resolve to nothing.

    A name resolves as a matrix case ID, a path that exists, or a function
    defined somewhere in the tree. Anything else is a dangling pointer: the
    case was renamed or deleted and the contract's claim to have a working
    oracle went stale without anything noticing.
    """
    dangling: list[str] = []
    for quoted in BACKTICKED.findall(inversion):
        for token in expand_braces(quoted.strip()):
            name = token.strip()
            if not name:
                continue
            if name in case_ids or name in fn_names or (PROJECT_ROOT / name).exists():
                continue
            dangling.append(name)
    return dangling


def check_fatal_contracts_have_inversions() -> None:
    """Every FATAL contract must name a case proving its oracle can fail, and
    that case must still exist.

    A test that has only ever passed cannot be told apart from one that cannot
    fail, and a suite of those reports PASS on a broken cluster. Checking only
    that the cell is non-empty made this table a promise nothing kept: a
    renamed or deleted inversion left the contract still claiming coverage.
    Every backticked name here is resolved against the matrix, the filesystem,
    and the function names in the tree.
    """
    rows = parse_contract_index(CONTRACTS_PATH.read_text(encoding="utf-8"))
    if not rows:
        raise AssertionError("Contract-to-Test Index not found or unparseable")

    case_ids = {str(case.get("id", "")) for case in json.loads(MATRIX_PATH.read_text())["cases"]}
    fn_names = _known_test_function_names()

    problems: list[str] = []
    fatal_seen = 0
    resolved_any = False
    for cells in rows:
        if len(cells) < 4:
            problems.append(f"row has {len(cells)} columns, expected 4: {cells[:1]}")
            continue
        contract, severity, _tests, inversion = cells[0], cells[1], cells[2], cells[3]
        if severity != "FATAL":
            continue
        fatal_seen += 1
        if not inversion or inversion == "—":
            problems.append(f"{contract} is FATAL with no inversion")
            continue
        if not BACKTICKED.findall(inversion):
            problems.append(
                f"{contract}'s inversion names nothing checkable: {inversion!r}. "
                f"Backtick the case ID, test function, or file that proves the "
                f"oracle can fail."
            )
            continue
        dangling = unresolved_inversion_refs(inversion, case_ids, fn_names)
        if dangling:
            problems.append(
                f"{contract}'s inversion points at {dangling}, which no longer exist "
                f"as matrix cases, files, or functions"
            )
        else:
            resolved_any = True

    if fatal_seen == 0:
        raise AssertionError("no FATAL rows parsed; the check would pass vacuously")
    if not resolved_any and not problems:
        raise AssertionError("no inversion reference resolved; the resolver is not working")
    if problems:
        raise AssertionError("; ".join(problems))


def cluster_topology_mismatches(cluster: dict[str, Any], derived: topology.Topology) -> list[str]:
    """Where the matrix's `cluster` block disagrees with the compose file.

    The runner builds its node map from the matrix, so this is the one place
    the topology is still written down twice. Reconciling it here is cheaper
    than threading the derivation through the runner, and it turns a silent
    divergence — a runner polling a management port nothing is listening on,
    reading no metrics, and concluding the node is down — into a lint failure.
    """
    problems: list[str] = []
    voters = {node.node_id: node for node in derived.voters}

    expected = cluster.get("expected_nodes")
    if expected != len(voters):
        problems.append(
            f"expected_nodes is {expected} but {derived.compose_file.name} declares "
            f"{len(voters)} voters"
        )

    declared = cluster.get("nodes") or []
    if {int(node["id"]) for node in declared} != set(voters):
        problems.append(
            f"matrix node ids {sorted(int(n['id']) for n in declared)} != "
            f"compose voter ids {sorted(voters)}"
        )
        return problems

    for node in declared:
        node_id = int(node["id"])
        real = voters[node_id]
        if node.get("name") != real.service:
            problems.append(f"node {node_id}: matrix name {node.get('name')!r} != {real.service!r}")
        if node.get("mgmt_url") != f"http://localhost:{real.mgmt_port}":
            problems.append(
                f"node {node_id}: mgmt_url {node.get('mgmt_url')!r} does not address the "
                f"published management port {real.mgmt_port}"
            )
        if node.get("metrics_url") != f"http://localhost:{real.metrics_port}/metrics":
            problems.append(
                f"node {node_id}: metrics_url {node.get('metrics_url')!r} does not address the "
                f"published metrics port {real.metrics_port}"
            )
    return problems


def check_matrix_cluster_matches_compose() -> None:
    """The matrix's cluster block must describe the cluster compose creates."""
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    compose_file = PROJECT_ROOT / data["compose_file"]
    problems = cluster_topology_mismatches(data["cluster"], topology.load(compose_file))
    if problems:
        raise AssertionError("; ".join(problems))


def transfers_that_cannot_wait_out_a_demote(
    cases: list[dict[str, Any]], min_retry_sec: int
) -> list[str]:
    """Transfers that give up before the target could finish demoting.

    A target still demoting refuses with 409 and the leader keeps heartbeating
    (`transfer_leadership` in `management_api/cluster.rs` asks readiness before
    it goes silent). The refusal means "not yet", so the runner retries within
    `retry_sec`; a budget shorter than a demote turns a working safety gate into
    a failed case.

    No wait can substitute for this. `min_healthy_replicas` reads the leader's
    replication view, which still shows the outgoing leader healthy for as long
    as it takes that node to notice it must demote.
    """
    problems: list[str] = []
    for case in cases:
        for index, action in enumerate(case.get("actions") or []):
            if action.get("type") != "transfer_leadership":
                continue
            retry_sec = int(action.get("retry_sec", DEFAULT_TRANSFER_RETRY_SEC))
            if retry_sec < min_retry_sec:
                problems.append(
                    f"{case['id']}: the transfer at action {index} (to node "
                    f"{action.get('target_node_id')}) retries for only {retry_sec}s, "
                    f"under the {min_retry_sec}s a target needs to finish demoting; "
                    f"a 409 means not-yet, and giving up on it fails the case for "
                    f"the gate working"
                )
    return problems


def runner_transfer_retry_default() -> int:
    """The default `retry_sec` as `ci_runner.py` actually reads it.

    Read rather than restated. A gate this file names but never checks against
    its source is how `min_healthy_replicas: 2` came to be documented as a
    settling guarantee it did not provide.
    """
    match = re.search(
        r"""step\.get\(\s*["']retry_sec["']\s*,\s*(\d+)\s*\)""",
        RUNNER_PATH.read_text(encoding="utf-8"),
    )
    if match is None:
        raise AssertionError(
            "could not find the retry_sec default in ci_runner.py; this check would "
            "otherwise pass while measuring a constant nothing reads"
        )
    return int(match.group(1))


def check_transfers_can_wait_out_a_demote() -> None:
    """A leadership transfer must outlast the target's demote before failing."""
    actual = runner_transfer_retry_default()
    if actual != DEFAULT_TRANSFER_RETRY_SEC:
        raise AssertionError(
            f"ci_runner.py defaults retry_sec to {actual}s, not the "
            f"{DEFAULT_TRANSFER_RETRY_SEC}s this lint assumes"
        )
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    problems = transfers_that_cannot_wait_out_a_demote(data["cases"], MIN_TRANSFER_RETRY_SEC)
    if problems:
        raise AssertionError(" | ".join(problems))


def implicit_build_targets(compose: topology.ComposeDocument, filename: str) -> list[str]:
    """Services that build from the Dockerfile without naming a stage."""
    problems: list[str] = []
    for name, service in compose.services.items():
        if service.build is None:
            continue
        if not service.build.target:
            problems.append(
                f"{filename}: service {name!r} builds without an explicit `target`, so it "
                f"gets whichever stage is last in the Dockerfile"
            )
    return problems


def check_build_targets_are_explicit() -> None:
    """Every building service must name the Dockerfile stage it wants.

    A bare ``build: .`` resolves to the last stage in the Dockerfile, so
    appending a stage silently repoints every such service at it. That is not
    hypothetical: adding ``runtime-lazyfs`` — which stays root so its entrypoint
    can mount FUSE — repointed the whole 3-node cluster at it, ``initdb``
    refused to run as root, and every node restart-looped. The build succeeded
    and the image was valid, so the only symptom was a cluster that never
    converged, thirteen minutes into every HA case.

    A stage name in the compose file is the ratchet: it cannot be invalidated
    by editing the Dockerfile somewhere else.
    """
    problems: list[str] = []
    for path in sorted(PROJECT_ROOT.glob("docker-compose*.yml")):
        compose = topology.ComposeDocument.model_validate(
            yaml.safe_load(path.read_text(encoding="utf-8"))
        )
        problems.extend(implicit_build_targets(compose, path.name))
    if problems:
        raise AssertionError("; ".join(problems))


RUST_SOURCE_ROOTS: Final[tuple[str, ...]] = ("src", "crates")


def _rust_corpus() -> str:
    """Every Rust source in the workspace, concatenated."""
    chunks: list[str] = []
    for root in RUST_SOURCE_ROOTS:
        for path in (PROJECT_ROOT / root).rglob("*.rs"):
            if "target" in path.parts:
                continue
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
    if not chunks:
        raise AssertionError(
            f"no Rust sources under {RUST_SOURCE_ROOTS}; every marker would read as missing"
        )
    return "\n".join(chunks)


def missing_log_markers(markers: Mapping[str, Sequence[str]], corpus: str) -> list[str]:
    """Markers the harness greps for that no longer appear in the Rust sources."""
    return [
        f"{group}: {marker!r}"
        for group, group_markers in markers.items()
        for marker in group_markers
        if marker not in corpus
    ]


def _harness_log_markers() -> dict[str, tuple[str, ...]]:
    import correctness_lite

    return {
        "LOG_LIVENESS_MARKERS": correctness_lite.LOG_LIVENESS_MARKERS,
        "LOG_SPLIT_BRAIN_SIGNALS": correctness_lite.LOG_SPLIT_BRAIN_SIGNALS,
        "LOG_FENCE_MARKERS": correctness_lite.LOG_FENCE_MARKERS,
        "LOG_FENCE_CONFIRMED": (correctness_lite.LOG_FENCE_CONFIRMED,),
        "LOG_FENCE_MOOT": (correctness_lite.LOG_FENCE_MOOT,),
    }


def check_log_markers_still_exist() -> None:
    """Every log line the harness greps for must still be emitted somewhere.

    `correctness_lite.py` reads L2 and L3 out of the container logs by matching
    strings that live in Rust `tracing` calls, and nothing connects the two
    copies. Rewording `"potential split-brain"` in `src/` would not fail
    anything — the grep would simply stop matching, and a run with a real
    split-brain signal in its logs would report PASS. That is the silent
    direction, so the strings are pinned here.

    Matching is exact and substring-based, the same way the harness matches, so
    this asserts precisely what the harness relies on.
    """
    missing = missing_log_markers(_harness_log_markers(), _rust_corpus())
    if missing:
        raise AssertionError(
            "log markers the harness greps for are no longer emitted: "
            + "; ".join(missing)
            + ". The grep would silently stop matching, and correctness_lite would "
            "report PASS on a log containing the real signal."
        )


def embedded_python(script: str) -> list[str]:
    """The `<<'PY' ... PY` blocks a shell script feeds to an interpreter."""
    return re.findall(r"<<'PY'[^\n]*\n(.*?)\n^PY$", script, re.S | re.M)


def harness_names_used(script: str) -> set[str]:
    """Names the Elle matrix reaches for on `linearizability_register`.

    The embedded blocks are parsed rather than grepped, so a shell variable that
    happens to be spelled `harness.log` is not mistaken for an attribute access.
    The alias comes from the `import ... as` line rather than being assumed,
    because renaming it would otherwise leave this check matching nothing.
    """
    blocks = embedded_python(script)
    if not blocks:
        raise AssertionError(
            f"{ELLE_MATRIX_PATH.name} has no embedded Python blocks; this check would "
            "match nothing and pass while blind"
        )
    used: set[str] = set()
    for block in blocks:
        tree = ast.parse(block)
        aliases = {
            alias.asname
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "linearizability_register" and alias.asname
        }
        used.update(
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
        )
    return used


def module_exports(source: str) -> set[str]:
    """Top-level names a module binds: imports, defs, classes, assignments.

    Read rather than imported. Importing `linearizability_register` drags in
    psycopg and every other runtime dependency, which would make this check fail
    for reasons that have nothing to do with the names it is pinning.
    """
    exports: set[str] = set()
    for node in ast.parse(source).body:
        match node:
            case ast.Import() | ast.ImportFrom():
                exports.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
            case ast.FunctionDef() | ast.AsyncFunctionDef() | ast.ClassDef():
                exports.add(node.name)
            case ast.Assign():
                exports.update(t.id for t in node.targets if isinstance(t, ast.Name))
            case ast.AnnAssign(target=ast.Name(id=name)):
                exports.add(name)
    return exports


def missing_harness_names(names: Iterable[str], exported: Container[str]) -> list[str]:
    """Names used by the matrix that the module does not export."""
    return sorted(name for name in names if name not in exported)


def check_elle_driver_names_resolve() -> None:
    """Every name the Elle matrix calls on the harness must still exist.

    `run_elle_matrix.sh` drives fault waves 2..N from a sibling process that
    imports `linearizability_register` and calls into it by attribute. The two
    files are joined by nothing, so `linreg`'s split left `find_leader` behind
    and the driver raised `AttributeError` on every attack for as long as it
    took a nightly run to say so.

    The direction that hurts is silence: the driver runs only under
    `ELLE_PROFILE=full`, which is nightly, so the PR smoke never exercises it.
    """
    missing = missing_harness_names(
        harness_names_used(ELLE_MATRIX_PATH.read_text(encoding="utf-8")),
        module_exports(LINEARIZABILITY_PATH.read_text(encoding="utf-8")),
    )
    if missing:
        raise AssertionError(
            f"{ELLE_MATRIX_PATH.name} calls names linearizability_register does not export: "
            + ", ".join(missing)
            + ". The fault-wave driver dies on AttributeError and every attack reports an "
            "infrastructure error instead of a verdict."
        )


def harness_subcommands_used(matrix_text: str) -> dict[str, str]:
    """`{script: subcommand}` for every `testing/<script>.py <word>` in the matrix.

    Only a bare word counts: an option (`--artifact-dir`) is an argument, not a
    subcommand.
    """
    found: dict[str, str] = {}
    for match in re.finditer(r"testing/([a-z_]+)\.py\s+([a-z][a-z-]*)\b", matrix_text):
        found[match.group(1)] = match.group(2)
    return found


def module_defines_subcommand(source: str, name: str) -> bool:
    """Whether `source` registers `name` as a Typer command.

    Matches the decorator and the function it names — a single-command app
    (`@app.command()` on one function, invoked with no verb) registers nothing
    a caller may pass as a word.
    """
    if not re.search(r"@app\.command\(", source):
        return False
    commands = re.findall(r"@app\.command\([^)]*\)\s*\ndef\s+([a-z_]+)", source)
    named = re.findall(r"@app\.command\(\s*[\"']([a-z-]+)[\"']", source)
    return name in named or (len(commands) > 1 and name in commands)


def check_harness_subcommands_exist() -> None:
    """A matrix step must not call a harness with a verb the harness rejects.

    `correctness-lite-invariants` called `correctness_lite.py run` against a
    single-command Typer app, which answers "Got unexpected extra argument
    (run)" and exits 2. The case had been failing on that alone, and could not
    say so, because it sits at position seventeen of a nightly suite that had
    not reached position eleven in months.
    """
    wrong: list[str] = []
    for script, verb in harness_subcommands_used(MATRIX_PATH.read_text(encoding="utf-8")).items():
        path = TESTING_DIR / f"{script}.py"
        if not path.exists():
            wrong.append(f"{script}.py does not exist")
            continue
        if not module_defines_subcommand(path.read_text(encoding="utf-8"), verb):
            wrong.append(f"{script}.py has no {verb!r} command")
    if wrong:
        raise AssertionError(
            "matrix steps call harnesses with arguments they reject: "
            + "; ".join(wrong)
            + ". The step exits 2 before doing anything, and the case reports that as its own "
            "failure."
        )


def check_ci_runs_every_case() -> None:
    """Every case is run by a workflow, or says in the matrix why it is not.

    `ha-ci.yml` used to restate the `ha-parallel` case list, and the copy fell
    twelve cases behind the suite: written, maintained, counted in `--list`, and
    executed by nothing. The workflow now derives its matrix from
    `--emit-cases`, so the drift cannot recur; what this guards is the escape
    hatch, since an exclusion with no reason is the same silence in a new place.
    """
    matrix = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    suited = {case_id for suite in matrix["suites"].values() for case_id in suite["cases"]}
    blank: list[str] = []
    orphaned: list[str] = []
    for case in matrix["cases"]:
        reason = case.get("ci_excluded_reason")
        if reason is not None and not reason.strip():
            blank.append(case["id"])
        if case["id"] not in suited and not reason:
            orphaned.append(case["id"])
    problems: list[str] = []
    if blank:
        problems.append(f"excluded with a blank reason: {', '.join(sorted(blank))}")
    if orphaned:
        problems.append(f"in no suite and unexplained: {', '.join(sorted(orphaned))}")
    if problems:
        raise AssertionError(
            "; ".join(problems) + ". A case CI never runs reads as coverage; give it a "
            "`ci_excluded_reason` saying why, or put it in a suite."
        )


def check_workflow_matrix_is_derived() -> None:
    """`ha-ci.yml` must build its parallel matrix from the suite, not restate it.

    A literal case list is exactly what went stale. This keeps the derivation in
    place rather than checking the two lists agree today.
    """
    workflow = (PROJECT_ROOT / ".github/workflows/ha-ci.yml").read_text(encoding="utf-8")
    if "--emit-cases" not in workflow:
        raise AssertionError(
            "ha-ci.yml no longer enumerates ha-parallel cases with --emit-cases. "
            "A hardcoded list drifts from the suite silently."
        )
    if "fromJSON(needs.ha-parallel-cases.outputs.cases)" not in workflow:
        raise AssertionError(
            "the ha-parallel job does not consume the enumerated case list, so the "
            "enumeration proves nothing about what runs."
        )


def check_fault_injection_confined() -> None:
    """Keep direct fault injection confined to the modules already tracked for it.

    A fault has to verify its own effect or it reads as coverage while injecting
    nothing; that verification lives in `fault_primitives.py`. This checks only
    which modules inject directly, not how many times, because a count is noise
    that churns on unrelated edits without saying anything more.
    """
    problems: list[str] = []
    # Includes package subdirectories: moving an attack into one would otherwise
    # walk straight out of this check.
    sources = sorted(TESTING_DIR.glob("*.py")) + sorted(TESTING_DIR.glob("*/*.py"))
    for path in sources:
        if path.name in FAULT_VERB_SCAN_EXEMPT or path.name.startswith("test_"):
            continue
        injects = bool(count_raw_fault_verbs(path.read_text(encoding="utf-8")))
        pending = path.name in PENDING_FAULT_MIGRATION
        if injects and not pending:
            problems.append(
                f"{path.name} injects faults directly; route them through "
                f"{PRIMITIVE_MODULE}, which verifies its own effect and resolves "
                f"docker names against the active compose project"
            )
        elif pending and not injects:
            problems.append(
                f"{path.name} no longer injects faults directly — drop it from "
                f"PENDING_FAULT_MIGRATION so it cannot regress unnoticed"
            )
    if problems:
        raise AssertionError(" | ".join(problems))


FILE_REFERENCE = re.compile(
    r"(?<![\w./-])((?:[\w.-]+/)*[\w.-]+\.(?:md|py|rs|sql|tla|toml|ya?ml|sh))(?![\w/])"
)
"""A token in case text that reads as a path to a file in this repository."""

CROSS_REF_PRUNE: Final = frozenset(
    {".git", ".venv", "__pycache__", "target", "node_modules", "backups", "states", "store"}
)
"""Directories that hold build output or run artifacts, not referenceable sources."""


def file_references(node: Any) -> set[str]:
    """Collect every path-shaped token from the strings anywhere under ``node``."""
    if isinstance(node, str):
        return set(FILE_REFERENCE.findall(node))
    if isinstance(node, Mapping):
        return set().union(*(file_references(v) for v in node.values())) if node else set()
    if isinstance(node, list):
        return set().union(*(file_references(v) for v in node)) if node else set()
    return set()


def unresolved_file_references(
    refs: Iterable[str], rel_paths: Container[str], basenames: Container[str]
) -> list[str]:
    """Return the references that name no file, by full path or by basename.

    Basenames count because case text says ``network.rs`` where the file is
    ``src/cluster/network.rs``, and demanding the full path would push authors
    toward writing no reference at all.
    """
    return sorted(
        {ref for ref in refs if ref not in rel_paths and Path(ref).name not in basenames}
    )


def repo_file_index() -> tuple[set[str], set[str]]:
    """Every referenceable file in the repository, by relative path and by name."""
    rel_paths: set[str] = set()
    basenames: set[str] = set()
    for dirpath, dirnames, filenames in PROJECT_ROOT.walk():
        dirnames[:] = [
            d for d in dirnames if d not in CROSS_REF_PRUNE and (d == ".github" or d[0] != ".")
        ]
        for name in filenames:
            rel_paths.add(str((dirpath / name).relative_to(PROJECT_ROOT)))
            basenames.add(name)
    return rel_paths, basenames


def check_case_cross_references_resolve() -> None:
    """Verify every file a case names is in the repository.

    Eighty cases carried a ``tests_md_ref`` naming ``docs/TESTS.md`` or
    ``BUGS.md``, neither of which was ever committed, and it held for eighty
    cases because nothing checked it. A case's own text is what somebody reads
    when the case and the product disagree about what should happen, so a
    reference that resolves to nothing is worse than no reference at all.
    """
    rel_paths, basenames = repo_file_index()
    data = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    problems = [
        f"{case['id']}: {', '.join(unresolved)}"
        for case in data["cases"]
        if (unresolved := unresolved_file_references(file_references(case), rel_paths, basenames))
    ]
    if problems:
        raise AssertionError(
            "case text names files that are not in the repository — " + " | ".join(problems)
        )


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
    check("Fault injection confined to tracked modules", check_fault_injection_confined)
    check("FATAL contracts declare an inversion", check_fatal_contracts_have_inversions)
    check("Matrix cluster matches docker-compose", check_matrix_cluster_matches_compose)
    check("Compose services pin a build target", check_build_targets_are_explicit)
    check("Transfers can wait out a demote", check_transfers_can_wait_out_a_demote)
    check("Log markers the harness greps for exist", check_log_markers_still_exist)
    check("Elle matrix driver names resolve", check_elle_driver_names_resolve)
    check("Harness subcommands the matrix calls exist", check_harness_subcommands_exist)
    check("CI runs every case, or says why not", check_ci_runs_every_case)
    check("Workflow matrix is derived from the suite", check_workflow_matrix_is_derived)
    check("Case cross-references name real files", check_case_cross_references_resolve)

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
