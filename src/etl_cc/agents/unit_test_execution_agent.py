"""Safe pytest execution with one durable result per test function."""

import ast
import json
import os
import subprocess
import sys
import time
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


def normalize_java_home(java_home: str | None) -> str | None:
    """Normalize JAVA_HOME when callers provide the JDK's bin directory."""
    if not java_home:
        return java_home
    normalized = java_home.strip().strip('"').strip("'")
    java_home_path = Path(normalized).expanduser()
    if java_home_path.name.lower() == "bin":
        return str(java_home_path.parent)
    return normalized


class TestCaseResult(BaseModel):
    test_id: str
    category: str = "UNIT_TEST"
    title: str
    status: str
    duration_seconds: float = 0.0
    details: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class UnitTestExecutionResult(BaseModel):
    status: str
    test_cases: list[TestCaseResult] = Field(default_factory=list)
    passed: int
    failed: int
    skipped: int
    stdout: str = ""
    stderr: str = ""


class UnitTestExecutionAgent:
    AGENT_NAME = "UNIT_TEST_EXECUTION_AGENT"
    AGENT_VERSION = "2.2.0"
    STAGE_NAME = "UNIT_TEST_EXECUTION"
    MODEL_NAME = "DETERMINISTIC_PYTEST_JSON"

    _ALLOWED_ENVIRONMENT_KEYS = {
        "PATH",
        "JAVA_HOME",
        "SPARK_HOME",
        "HADOOP_HOME",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "HOME",
        "LOCALAPPDATA",
        "APPDATA",
        "PYSPARK_SUBMIT_ARGS",
    }

    def run(
        self,
        test_file: Path,
        timeout_seconds: int,
    ) -> UnitTestExecutionResult:
        test_file = Path(test_file).resolve()
        if not test_file.is_file():
            return self._single_failure(
                "Unit-test artifact is missing.",
                evidence={"test_file": str(test_file)},
            )

        shape_errors = self._validate_create_dataframe_shapes(test_file)
        if shape_errors:
            return self._single_failure(
                "; ".join(shape_errors),
                evidence={
                    "test_file": str(test_file),
                    "preflight_check": "CREATE_DATAFRAME_ROW_SCHEMA_SHAPE",
                },
            )

        report_directory = tempfile.TemporaryDirectory(prefix="neuflow-pytest-")
        report_path = Path(report_directory.name) / "pytest-report.json"

        safe_env = self._build_safe_environment(test_file.parent)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            test_file.name,
            "--json-report",
            f"--json-report-file={report_path}",
        ]

        started = time.perf_counter()
        try:
            completed = subprocess.run(
                command,
                cwd=test_file.parent,
                env=safe_env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return self._single_failure(
                f"Test execution timed out after {timeout_seconds} seconds.",
                duration=float(timeout_seconds),
                stdout=self._output_text(exc.stdout),
                stderr=self._output_text(exc.stderr),
                evidence={
                    "command": command,
                    "test_file": str(test_file),
                    "timeout_seconds": timeout_seconds,
                },
            )
        except OSError as exc:
            return self._single_failure(
                f"pytest process could not be started: {type(exc).__name__}: {exc}",
                duration=round(time.perf_counter() - started, 4),
                evidence={
                    "command": command,
                    "test_file": str(test_file),
                },
            )

        elapsed = round(time.perf_counter() - started, 4)
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""

        if not report_path.is_file():
            plugin_missing = self._json_report_plugin_missing(stdout, stderr)
            details = (
                "pytest JSON report was not generated. "
                f"pytest exit code: {completed.returncode}. "
            )
            if plugin_missing:
                details += (
                    "The pytest-json-report plugin is unavailable in the same "
                    "virtual environment used by the Validation Worker."
                )
            else:
                details += (
                    "pytest did not start or terminated before writing the report. "
                    "Review stdout and stderr evidence."
                )
            return self._single_failure(
                details,
                duration=elapsed,
                stdout=stdout,
                stderr=stderr,
                evidence={
                    "command": command,
                    "exit_code": completed.returncode,
                    "test_file": str(test_file),
                    "report_path": str(report_path),
                    "pytest_json_report_missing": plugin_missing,
                },
            )

        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            return self._single_failure(
                "pytest generated an invalid or unreadable JSON report. "
                f"{type(exc).__name__}: {exc}",
                duration=elapsed,
                stdout=stdout,
                stderr=stderr,
                evidence={
                    "command": command,
                    "exit_code": completed.returncode,
                    "test_file": str(test_file),
                    "report_path": str(report_path),
                },
            )
        finally:
            report_directory.cleanup()

        cases: list[TestCaseResult] = []
        for index, item in enumerate(report.get("tests", []), start=1):
            outcome = str(item.get("outcome", "failed")).upper()
            status = {
                "PASSED": "PASSED",
                "SKIPPED": "SKIPPED",
                "XFAILED": "SKIPPED",
            }.get(outcome, "FAILED")

            phase = item.get("call") or item.get("setup") or item.get("teardown") or {}
            longrepr = phase.get("longrepr")
            details = "Test completed successfully."
            if status == "SKIPPED":
                details = str(longrepr or "Test was skipped.")[-4000:]
            elif status == "FAILED":
                details = str(longrepr or "Test failed.")[-4000:]

            node_id = str(item.get("nodeid", f"test_{index}"))
            duration = self._test_duration(item)
            cases.append(
                TestCaseResult(
                    test_id=f"UNIT-{index:03d}",
                    title=node_id.split("::")[-1],
                    status=status,
                    duration_seconds=duration,
                    details=details,
                    evidence={
                        "node_id": node_id,
                        "line": item.get("lineno"),
                        "outcome": item.get("outcome"),
                        "pytest_exit_code": completed.returncode,
                        "stdout": stdout[-4000:],
                        "stderr": stderr[-4000:],
                    },
                )
            )

        if not cases:
            summary = report.get("summary", {})
            cases.append(
                TestCaseResult(
                    test_id="UNIT-001",
                    title="Generated PySpark unit tests",
                    status="FAILED",
                    duration_seconds=elapsed,
                    details="No pytest test functions were collected.",
                    evidence={
                        "command": command,
                        "exit_code": completed.returncode,
                        "summary": summary,
                        "stdout": stdout[-4000:],
                        "stderr": stderr[-4000:],
                    },
                )
            )

        passed = sum(item.status == "PASSED" for item in cases)
        failed = sum(item.status == "FAILED" for item in cases)
        skipped = sum(item.status == "SKIPPED" for item in cases)

        overall_status = (
            "PASSED"
            if completed.returncode == 0 and failed == 0 and passed > 0
            else "FAILED"
        )
        return UnitTestExecutionResult(
            status=overall_status,
            test_cases=cases,
            passed=passed,
            failed=failed,
            skipped=skipped,
            stdout=stdout[-10000:],
            stderr=stderr[-10000:],
        )

    @staticmethod
    def _validate_create_dataframe_shapes(test_file: Path) -> list[str]:
        """Reject literal createDataFrame row/schema width mismatches."""
        try:
            tree = ast.parse(test_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, SyntaxError) as exc:
            return [f"Unit-test preflight parsing failed: {type(exc).__name__}: {exc}"]

        assignments: dict[str, ast.AST] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                if isinstance(node.targets[0], ast.Name):
                    assignments[node.targets[0].id] = node.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.value is not None:
                    assignments[node.target.id] = node.value

        def resolve(node: ast.AST) -> ast.AST:
            return assignments.get(node.id, node) if isinstance(node, ast.Name) else node

        def schema_width(node: ast.AST) -> int | None:
            node = resolve(node)
            if not isinstance(node, ast.Call) or not node.args:
                return None
            name = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else ""
            )
            fields = resolve(node.args[0])
            return len(fields.elts) if name == "StructType" and isinstance(fields, (ast.List, ast.Tuple)) else None

        def row_widths(node: ast.AST) -> set[int] | None:
            node = resolve(node)
            if not isinstance(node, (ast.List, ast.Tuple)):
                return None
            widths: set[int] = set()
            for row in node.elts:
                row = resolve(row)
                if not isinstance(row, (ast.List, ast.Tuple)):
                    return None
                widths.add(len(row.elts))
            return widths

        errors: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            name = node.func.attr if isinstance(node.func, ast.Attribute) else (
                node.func.id if isinstance(node.func, ast.Name) else ""
            )
            if name != "createDataFrame":
                continue
            widths = row_widths(node.args[0])
            width = schema_width(node.args[1])
            if widths is not None and width is not None and any(item != width for item in widths):
                errors.append(
                    "Generated UNIT_TEST is invalid: createDataFrame StructType "
                    f"has {width} fields but row widths are {sorted(widths)}."
                )
        return errors

    @classmethod
    def _build_safe_environment(cls, test_directory: Path) -> dict[str, str]:
        safe_env = {
            key: value
            for key, value in os.environ.items()
            if key.upper() in cls._ALLOWED_ENVIRONMENT_KEYS
        }
        java_home = safe_env.get("JAVA_HOME")
        if java_home:
            safe_env["JAVA_HOME"] = normalize_java_home(java_home) or java_home
        existing_python_path = os.environ.get("PYTHONPATH", "")
        python_path_parts = [str(test_directory)]
        if existing_python_path:
            python_path_parts.append(existing_python_path)

        safe_env.update(
            {
                "PYTHONPATH": os.pathsep.join(python_path_parts),
                "PYTHONIOENCODING": "utf-8",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYSPARK_PYTHON": sys.executable,
                "PYSPARK_DRIVER_PYTHON": sys.executable,
            }
        )
        return safe_env

    @staticmethod
    def _json_report_plugin_missing(stdout: str, stderr: str) -> bool:
        combined = (stdout + "\n" + stderr).lower()
        markers = (
            "unrecognized arguments: --json-report",
            "unrecognized arguments: --json-report-file",
            "pytest-json-report",
        )
        return any(marker in combined for marker in markers)

    @staticmethod
    def _test_duration(item: dict[str, Any]) -> float:
        direct = item.get("duration")
        if direct is not None:
            return round(float(direct), 4)
        total = 0.0
        for phase_name in ("setup", "call", "teardown"):
            phase = item.get(phase_name) or {}
            total += float(phase.get("duration", 0.0) or 0.0)
        return round(total, 4)

    @staticmethod
    def _output_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    @staticmethod
    def skipped(reason: str) -> UnitTestExecutionResult:
        return UnitTestExecutionResult(
            status="SKIPPED",
            passed=0,
            failed=0,
            skipped=1,
            test_cases=[
                TestCaseResult(
                    test_id="UNIT-001",
                    title="Generated PySpark unit tests",
                    status="SKIPPED",
                    details=reason,
                )
            ],
        )

    @staticmethod
    def _single_failure(
        details: str,
        duration: float = 0.0,
        stdout: str = "",
        stderr: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> UnitTestExecutionResult:
        stdout = stdout or ""
        stderr = stderr or ""
        failure_evidence = dict(evidence or {})
        failure_evidence.update(
            {
                "stdout": stdout[-4000:],
                "stderr": stderr[-4000:],
            }
        )
        return UnitTestExecutionResult(
            status="FAILED",
            passed=0,
            failed=1,
            skipped=0,
            stdout=stdout[-10000:],
            stderr=stderr[-10000:],
            test_cases=[
                TestCaseResult(
                    test_id="UNIT-001",
                    title="Generated PySpark unit tests",
                    status="FAILED",
                    duration_seconds=duration,
                    details=details,
                    evidence=failure_evidence,
                )
            ],
        )
