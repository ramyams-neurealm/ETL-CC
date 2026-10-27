"""Persistence helpers for UI-ready validation test cases."""

from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.models import ValidationTestCaseETL


async def replace_planned_test_cases(
    session: AsyncSession,
    workflow_run_id: int,
    etl_object_id: int,
    planned_cases: list,
) -> None:
    await session.execute(
        delete(ValidationTestCaseETL).where(
            ValidationTestCaseETL.workflow_run_id == workflow_run_id,
            ValidationTestCaseETL.etl_object_id == etl_object_id,
        )
    )
    for item in planned_cases:
        session.add(
            ValidationTestCaseETL(
                workflow_run_id=workflow_run_id,
                etl_object_id=etl_object_id,
                test_id=item.test_id,
                test_name=item.test_name,
                category=item.category,
                status="PLANNED",
                severity=item.severity,
                expected_result=item.expected_result,
                evidence_payload=item.evidence_payload,
            )
        )
    await session.flush()


async def upsert_test_case(
    session: AsyncSession,
    *,
    workflow_run_id: int,
    etl_object_id: int,
    agent_response_id: int | None,
    test_id: str,
    test_name: str,
    category: str,
    status: str,
    severity: str,
    expected_result: str,
    actual_result: str,
    details: str,
    evidence_payload: dict,
    duration_seconds: float = 0.0,
) -> ValidationTestCaseETL:
    row = await session.scalar(
        select(ValidationTestCaseETL).where(
            ValidationTestCaseETL.workflow_run_id == workflow_run_id,
            ValidationTestCaseETL.etl_object_id == etl_object_id,
            ValidationTestCaseETL.test_id == test_id,
        )
    )
    if row is None:
        row = ValidationTestCaseETL(
            workflow_run_id=workflow_run_id,
            etl_object_id=etl_object_id,
            test_id=test_id,
            test_name=test_name,
            category=category,
        )
        session.add(row)
    row.agent_response_id = agent_response_id
    row.test_name = test_name
    row.category = category
    row.status = status
    row.severity = severity
    row.expected_result = expected_result
    row.actual_result = actual_result
    row.details = details
    row.evidence_payload = evidence_payload
    row.duration_milliseconds = max(0, int(duration_seconds * 1000))
    row.updated_at = datetime.now(timezone.utc)
    await session.flush()
    return row
