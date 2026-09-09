"""Safe pytest execution with one durable result per test function."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from pydantic import BaseModel, Field


class TestCaseResult(BaseModel):
    test_id: str
    category: str = "UNIT_TEST"
    title: str
    status: str
    duration_seconds: float = 0.0
    details: str
    evidence: dict = Field(default_factory=dict)


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
    AGENT_VERSION = "2.0.0"
    STAGE_NAME = "UNIT_TEST_EXECUTION"
    MODEL_NAME = "DETERMINISTIC_PYTEST_JSON"

    def run(self, test_file: Path, timeout_seconds: int) -> UnitTestExecutionResult:
        if not test_file.is_file():
            return self._single_failure("Unit-test artifact is missing.")

        report_path = test_file.parent / ".pytest-report.json"
        report_path.unlink(missing_ok=True)
        safe_env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(test_file.parent),
            "PYTHONIOENCODING": "utf-8",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
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
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return self._single_failure(
                "Test execution timed out.",
                duration=float(timeout_seconds),
            )

        elapsed = round(time.perf_counter() - started, 4)
        if not report_path.is_file():
            return self._single_failure(
                "pytest JSON report was not generated. Ensure pytest-json-report is installed.",
                duration=elapsed,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )

        report = json.loads(report_path.read_text(encoding="utf-8"))
        cases: list[TestCaseResult] = []
        for index, item in enumerate(report.get("tests", []), start=1):
            outcome = str(item.get("outcome", "failed")).upper()
            status = {
                "PASSED": "PASSED",
                "SKIPPED": "SKIPPED",
                "XFAILED": "SKIPPED",
            }.get(outcome, "FAILED")
            call = item.get("call") or item.get("setup") or {}
            longrepr = call.get("longrepr")
            details = "Test completed successfully."
            if status == "SKIPPED":
                details = "Test was skipped."
            elif status == "FAILED":
                details = str(longrepr or "Test failed.")[-4000:]
            node_id = str(item.get("nodeid", f"test_{index}"))
            cases.append(
                TestCaseResult(
                    test_id=f"UNIT-{index:03d}",
                    title=node_id.split("::")[-1],
                    status=status,
                    duration_seconds=round(float(item.get("duration", 0.0)), 4),
                    details=details,
                    evidence={
                        "node_id": node_id,
                        "line": item.get("lineno"),
                        "outcome": item.get("outcome"),
                    },
                )
            )

        if not cases:
            cases.append(
                TestCaseResult(
                    test_id="UNIT-001",
                    title="Generated PySpark unit tests",
                    status="FAILED",
                    duration_seconds=elapsed,
                    details="No pytest test functions were collected.",
                )
            )

        passed = sum(item.status == "PASSED" for item in cases)
        failed = sum(item.status == "FAILED" for item in cases)
        skipped = sum(item.status == "SKIPPED" for item in cases)
        return UnitTestExecutionResult(
            status="PASSED" if failed == 0 and passed > 0 else "FAILED",
            test_cases=cases,
            passed=passed,
            failed=failed,
            skipped=skipped,
            stdout=completed.stdout[-10000:],
            stderr=completed.stderr[-10000:],
        )

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
    ) -> UnitTestExecutionResult:
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
                )
            ],
        )
