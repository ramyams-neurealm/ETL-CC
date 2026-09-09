"""Deterministic static validation for generated conversion artifacts."""

import ast
import json
import re
from pathlib import Path
from pydantic import BaseModel, Field


class ValidationCheck(BaseModel):
    check_name: str
    passed: bool
    severity: str = "HIGH"
    details: str


class StaticValidationResult(BaseModel):
    status: str
    checks: list[ValidationCheck] = Field(default_factory=list)
    passed_count: int
    failed_count: int
    safe_to_execute_tests: bool


class StaticValidationAgent:
    AGENT_NAME = "STATIC_VALIDATION_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "STATIC_VALIDATION"
    MODEL_NAME = "DETERMINISTIC_AST"

    def run(self, artifacts: dict[str, Path]) -> StaticValidationResult:
        checks: list[ValidationCheck] = []
        required = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
        missing = sorted(required - set(artifacts))
        checks.append(ValidationCheck(check_name="REQUIRED_ARTIFACTS", passed=not missing, details="All required artifacts are present." if not missing else f"Missing: {', '.join(missing)}"))

        contents = {key: path.read_text(encoding="utf-8") for key, path in artifacts.items() if path.is_file()}
        for kind in ("PYSPARK_CODE", "UNIT_TEST"):
            content = contents.get(kind, "")
            try:
                ast.parse(content)
                passed = bool(content)
                details = f"{kind} parses as Python." if passed else f"{kind} is empty."
            except SyntaxError as exc:
                passed = False
                details = f"Syntax error at line {exc.lineno}: {exc.msg}"
            checks.append(ValidationCheck(check_name=f"{kind}_SYNTAX", passed=passed, details=details))

        config_text = contents.get("CONFIGURATION", "")
        try:
            config = json.loads(config_text)
            config_ok = isinstance(config, dict)
            config_details = "Configuration contains valid JSON object."
        except (json.JSONDecodeError, TypeError) as exc:
            config_ok = False
            config_details = f"Invalid configuration JSON: {exc}"
        checks.append(ValidationCheck(check_name="CONFIGURATION_JSON", passed=config_ok, details=config_details))

        combined = "\n".join(contents.values())
        forbidden = {
            "/path/to/": "hardcoded placeholder path",
            "SparkSession.builder": "mapping creates a Spark session",
            "spark.stop()": "mapping stops the shared Spark session",
            "TODO": "TODO placeholder",
            "<PLACEHOLDER>": "placeholder token",
        }
        found = [description for token, description in forbidden.items() if token.lower() in combined.lower()]
        checks.append(ValidationCheck(check_name="DATABRICKS_RUNTIME_SAFETY", passed=not found, details="No unsafe Databricks runtime patterns found." if not found else "Found: " + ", ".join(found)))

        secret_patterns = [r"OPENAI_API_KEY", r"AZURE_CLIENT_SECRET", r"password\s*=", r"sk-[A-Za-z0-9_-]{20,}"]
        secret_found = [pattern for pattern in secret_patterns if re.search(pattern, combined, flags=re.I)]
        checks.append(ValidationCheck(check_name="NO_EMBEDDED_SECRETS", passed=not secret_found, details="No secret markers found." if not secret_found else "Potential secret patterns found."))

        dangerous_imports = {"subprocess", "socket", "requests", "httpx", "urllib", "ftplib"}
        imported: set[str] = set()
        for kind in ("PYSPARK_CODE", "UNIT_TEST"):
            try:
                tree = ast.parse(contents.get(kind, ""))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".")[0])
        prohibited = sorted(imported & dangerous_imports)
        checks.append(ValidationCheck(check_name="SAFE_TEST_IMPORTS", passed=not prohibited, details="No prohibited imports found." if not prohibited else f"Prohibited imports: {', '.join(prohibited)}"))

        failed = sum(not check.passed for check in checks)
        return StaticValidationResult(status="PASSED" if failed == 0 else "FAILED", checks=checks, passed_count=len(checks)-failed, failed_count=failed, safe_to_execute_tests=failed == 0)
