import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from etl_cc.agents.validation_agent import (
    SchemaContractValidator,
    TargetPySparkExecutor,
    ValidationAgent,
    ValidationContext,
)
from etl_cc.models import CanonicalDataset, CanonicalField, CanonicalMapping
from etl_cc.validation_store import ValidationStore


def _mapping() -> CanonicalMapping:
    return CanonicalMapping(
        source_vendor="TEST",
        source_object_key="TEST.MAPPING",
        mapping_name="TEST_MAPPING",
        sources=[
            CanonicalDataset(
                name="SRC",
                dataset_type="TABLE",
                fields=[
                    CanonicalField(name="id", data_type="integer"),
                    CanonicalField(name="name", data_type="string"),
                    CanonicalField(name="salary", data_type="decimal"),
                ],
            )
        ],
        targets=[
            CanonicalDataset(
                name="TGT",
                dataset_type="TABLE",
                fields=[CanonicalField(name="id", data_type="integer")],
            )
        ],
    )


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    result = {}
    for artifact_type in ("PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"):
        path = tmp_path / f"{artifact_type}.txt"
        path.write_text("", encoding="utf-8")
        result[artifact_type] = path
    return result


def test_schema_contract_accepts_reordered_columns_and_warns_on_extras():
    result = SchemaContractValidator.run(
        _mapping(),
        {
            "SRC": [
                {"salary": 1000, "name": "Vignesh", "id": 1, "extra": "ok"}
            ]
        },
        dataset_assignments=[{"dataset_name": "SRC", "upload_id": "upload-1"}],
        input_dataset_schemas={
            "SRC": [
                {"name": "salary", "data_type": "integer"},
                {"name": "name", "data_type": "string"},
                {"name": "id", "data_type": "integer"},
                {"name": "extra", "data_type": "string"},
            ]
        },
        input_mode="DATASET_FILE",
    )

    assert result.status == "PASSED"
    assert result.errors == []
    assert [issue["code"] for issue in result.warnings] == ["UNEXPECTED_COLUMN"]


def test_schema_validation_failure_skips_target_execution(tmp_path, monkeypatch):
    def unexpected_execution(*args, **kwargs):
        raise AssertionError("TargetPySparkExecutor must not run after schema failure.")

    monkeypatch.setattr(TargetPySparkExecutor, "run", unexpected_execution)
    result = asyncio.run(
        ValidationAgent().run(
            ValidationContext(
                workflow_id="VAL-TEST",
                mapping=_mapping(),
                artifact_paths=_artifact_paths(tmp_path),
                input_mode="DATASET_FILE",
                input_datasets={"SRC": [{"id": 1, "salary": 1000}]},
                dataset_assignments=[
                    {"dataset_name": "SRC", "upload_id": "upload-1"}
                ],
                input_dataset_schemas={
                    "SRC": [
                        {"name": "id", "data_type": "integer"},
                        {"name": "salary", "data_type": "integer"},
                    ]
                },
            )
        )
    )

    assert result.status == "FAILED"
    assert result.schema_validation == "FAILED"
    assert result.target_execution == "SKIPPED"
    schema_stage = next(
        stage for stage in result.stages if stage.stage_name == "SCHEMA_VALIDATION"
    )
    assert schema_stage.response_payload["errors"][0]["code"] == "MISSING_COLUMN"
    assert next(test for test in result.test_cases if test.test_id == "DATA-003").status == "SKIPPED"


def test_schema_validation_rejects_incompatible_datatype():
    result = SchemaContractValidator.run(
        _mapping(),
        {"SRC": [{"id": 1, "name": "Vignesh", "salary": "1000"}]},
        dataset_assignments=[{"dataset_name": "SRC", "upload_id": "upload-1"}],
        input_dataset_schemas={
            "SRC": [
                {"name": "id", "data_type": "integer"},
                {"name": "name", "data_type": "string"},
                {"name": "salary", "data_type": "string"},
            ]
        },
        input_mode="DATASET_FILE",
    )

    assert result.status == "FAILED"
    assert any(
        issue["code"] == "INCOMPATIBLE_DATATYPE"
        and issue["column"] == "salary"
        for issue in result.errors
    )


def test_schema_validation_rejects_bad_assignments_before_execution(
    tmp_path, monkeypatch
):
    def unexpected_execution(*args, **kwargs):
        raise AssertionError("TargetPySparkExecutor must not run for invalid assignments.")

    monkeypatch.setattr(TargetPySparkExecutor, "run", unexpected_execution)
    result = asyncio.run(
        ValidationAgent().run(
            ValidationContext(
                workflow_id="VAL-TEST",
                mapping=_mapping(),
                artifact_paths=_artifact_paths(tmp_path),
                input_mode="DATASET_FILE",
                input_datasets={},
                dataset_assignments=[
                    {"dataset_name": "OTHER", "upload_id": "upload-1"},
                    {
                        "dataset_name": "SRC",
                        "upload_id": "upload-1",
                        "load_error": "Validation upload was not found.",
                        "load_error_code": "UPLOAD_NOT_FOUND",
                    },
                ],
            )
        )
    )

    schema_stage = next(
        stage for stage in result.stages if stage.stage_name == "SCHEMA_VALIDATION"
    )
    codes = {issue["code"] for issue in schema_stage.response_payload["errors"]}
    assert result.status == "FAILED"
    assert result.schema_validation == "FAILED"
    assert result.target_execution == "SKIPPED"
    assert "UNKNOWN_DATASET" in codes
    assert "DUPLICATE_UPLOAD_ASSIGNMENT" in codes
    assert "UPLOAD_NOT_FOUND" in codes


def test_schema_validation_rejects_duplicate_source_assignments_and_columns():
    duplicate_assignment = SchemaContractValidator.run(
        _mapping(),
        {"SRC": [{"id": 1, "name": "Vignesh", "salary": 1000}]},
        dataset_assignments=[
            {"dataset_name": "SRC", "upload_id": "upload-1"},
            {"dataset_name": "SRC", "upload_id": "upload-2"},
        ],
        input_mode="DATASET_FILE",
    )
    duplicate_columns = SchemaContractValidator.run(
        _mapping(),
        {"SRC": [{"id": 1, "id.1": 2, "name": "Vignesh", "salary": 1000}]},
        dataset_assignments=[{"dataset_name": "SRC"}],
        input_dataset_schemas={
            "SRC": [
                {"name": "id", "data_type": "integer"},
                {"name": "id", "data_type": "integer"},
                {"name": "name", "data_type": "string"},
                {"name": "salary", "data_type": "integer"},
            ]
        },
        input_mode="DATASET_FILE",
    )

    assert any(
        issue["code"] == "DUPLICATE_SOURCE_ASSIGNMENT"
        for issue in duplicate_assignment.errors
    )
    assert any(
        issue["code"] == "DUPLICATE_COLUMN"
        for issue in duplicate_columns.errors
    )


def test_validation_store_preserves_duplicate_csv_headers_for_contract_checking():
    upload = ValidationStore().inspect(
        "duplicate.csv",
        b"id,id,name\n1,2,Vignesh\n",
    )

    assert [field["name"] for field in upload["schema"]] == ["id", "id", "name"]


def test_schema_validation_rejects_invalid_date_values():
    mapping = _mapping()
    mapping.sources[0].fields.append(
        CanonicalField(name="effective_date", data_type="date")
    )
    result = SchemaContractValidator.run(
        mapping,
        {
            "SRC": [
                {
                    "id": 1,
                    "name": "Vignesh",
                    "salary": 1000,
                    "effective_date": "not-a-date",
                }
            ]
        },
        dataset_assignments=[{"dataset_name": "SRC"}],
        input_dataset_schemas={
            "SRC": [
                {"name": "id", "data_type": "integer"},
                {"name": "name", "data_type": "string"},
                {"name": "salary", "data_type": "integer"},
                {"name": "effective_date", "data_type": "string"},
            ]
        },
        input_mode="DATABASE_TABLES",
    )

    assert any(
        issue["code"] == "INVALID_DATE_VALUE"
        and issue["column"] == "effective_date"
        for issue in result.errors
    )


def test_target_execution_success_sets_data_003_passed(tmp_path, monkeypatch):
    code_path = tmp_path / "transform.py"
    code_path.write_text("def transform(inputs): return {}\n", encoding="utf-8")
    schema = SchemaContractValidator.run(
        _mapping(),
        {"SRC": [{"id": 1, "name": "Vignesh", "salary": 1000}]},
        dataset_assignments=[{"dataset_name": "SRC", "upload_id": "upload-1"}],
        input_dataset_schemas={
            "SRC": [
                {"name": "id", "data_type": "integer"},
                {"name": "name", "data_type": "string"},
                {"name": "salary", "data_type": "integer"},
            ]
        },
        input_mode="DATASET_FILE",
    )

    def successful_subprocess(command, **kwargs):
        Path(command[-1]).write_text(json.dumps({"TGT": [{"id": 1}]}), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(
        "etl_cc.agents.validation_agent.subprocess.run", successful_subprocess
    )
    target = TargetPySparkExecutor().run(
        code_path,
        _mapping(),
        {"SRC": [{"id": 1, "name": "Vignesh", "salary": 1000}]},
        timeout=1,
    )
    data_tests = ValidationAgent._data_tests(
        SimpleNamespace(status="PASSED", checks=[]),
        SimpleNamespace(
            status="PASSED",
            error_message=None,
            reference_execution_mode="RELATIONAL_IR",
            oracle_strength="STRONG",
            full_reference_coverage=True,
            human_review_required=False,
            unsupported_constructs=[],
            row_count=1,
        ),
        target,
        None,
        1.0,
        0.95,
    )

    assert schema.status == "PASSED"
    assert target.status == "PASSED"
    assert next(test for test in data_tests if test.test_id == "DATA-003").status == "PASSED"


def test_target_execution_runtime_exception_and_timeout_are_distinct(
    tmp_path, monkeypatch
):
    code_path = tmp_path / "transform.py"
    code_path.write_text("def transform(inputs): return {}\n", encoding="utf-8")

    def failed_subprocess(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="out", stderr="boom")

    monkeypatch.setattr(
        "etl_cc.agents.validation_agent.subprocess.run", failed_subprocess
    )
    runtime_failure = TargetPySparkExecutor().run(
        code_path,
        _mapping(),
        {"SRC": []},
        timeout=1,
    )
    assert runtime_failure.status == "FAILED"
    assert runtime_failure.error_type == "TRANSFORM_EXCEPTION"
    assert runtime_failure.stdout == "out"
    assert runtime_failure.stderr == "boom"

    def timed_out_subprocess(command, **kwargs):
        raise subprocess.TimeoutExpired(command, timeout=1)

    monkeypatch.setattr(
        "etl_cc.agents.validation_agent.subprocess.run", timed_out_subprocess
    )
    timeout_failure = TargetPySparkExecutor().run(
        code_path,
        _mapping(),
        {"SRC": []},
        timeout=1,
    )
    assert timeout_failure.status == "FAILED"
    assert timeout_failure.error_type == "EXECUTION_TIMEOUT"
