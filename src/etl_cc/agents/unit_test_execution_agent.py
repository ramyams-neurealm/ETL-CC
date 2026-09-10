"""Safe pytest execution with one durable result per test function."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


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
    AGENT_VERSION = "2.1.0"
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

        report_path = test_file.parent / ".pytest-report.json"
        report_path.unlink(missing_ok=True)

        safe_env = self._build_safe_environment(test_file.parent)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            test_file.name,
            "--json-report",
            f"--json-report-file={report_path.name}",
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
            report_path.unlink(missing_ok=True)

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

    @classmethod
    def _build_safe_environment(cls, test_directory: Path) -> dict[str, str]:
        safe_env = {
            key: value
            for key, value in os.environ.items()
            if key.upper() in cls._ALLOWED_ENVIRONMENT_KEYS
        }
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
