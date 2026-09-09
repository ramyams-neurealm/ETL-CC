"""Worker for detailed static, unit-test, and functional-parity validation."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.functional_parity_agent import FunctionalParityAgent
from etl_cc.agents.static_validation_agent import StaticValidationAgent
from etl_cc.agents.test_plan_agent import TestPlanAgent
from etl_cc.agents.unit_test_execution_agent import UnitTestExecutionAgent
from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.models import (
    AgentResponseETL,
    CanonicalMapping,
    ETLObjectETL,
    GeneratedArtifactETL,
    WorkflowEventETL,
    WorkflowRunETL,
)
from etl_cc.validation_test_case_service import (
    replace_planned_test_cases,
    upsert_test_case,
)


STATIC_TEST_IDS = {
    "REQUIRED_ARTIFACTS": "STATIC-001",
    "PYSPARK_CODE_SYNTAX": "STATIC-002",
    "UNIT_TEST_SYNTAX": "STATIC-003",
    "CONFIGURATION_JSON": "STATIC-004",
    "DATABRICKS_RUNTIME_SAFETY": "STATIC-005",
    "NO_EMBEDDED_SECRETS": "STATIC-006",
    "SAFE_TEST_IMPORTS": "STATIC-007",
}


async def _claim() -> int | None:
    """Atomically claim one queued Validation workflow."""
    async with SessionFactory() as session:
        async with session.begin():
            workflow = await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_status == "QUEUED",
                    WorkflowRunETL.job_type == "VALIDATION",
                )
                .order_by(
                    WorkflowRunETL.priority,
                    WorkflowRunETL.created_at,
                )
                .with_for_update(skip_locked=True)
                .limit(1)
            )

            if workflow is None:
                return None

            workflow.job_status = "RUNNING"
            workflow.overall_status = "RUNNING"
            workflow.current_stage = "VALIDATION"
            workflow.started_at = datetime.now(timezone.utc)
            workflow.attempt_count += 1

            return workflow.id


async def _audit(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    agent: Any,
    result: Any,
) -> AgentResponseETL:
    """Persist one deterministic agent execution result."""
    attempt = int(
        await session.scalar(
            select(
                func.coalesce(
                    func.max(AgentResponseETL.attempt_number),
                    0,
                )
            ).where(
                AgentResponseETL.workflow_run_id == workflow.id,
                AgentResponseETL.etl_object_id == mapping.id,
                AgentResponseETL.agent_name == agent.AGENT_NAME,
            )
        )
        or 0
    ) + 1

    now = datetime.now(timezone.utc)
    row = AgentResponseETL(
        workflow_run_id=workflow.id,
        etl_object_id=mapping.id,
        agent_name=agent.AGENT_NAME,
        agent_version=agent.AGENT_VERSION,
        stage_name=agent.STAGE_NAME,
        attempt_number=attempt,
        status="COMPLETED",
        request_payload={"etl_object_id": mapping.id},
        response_payload=result.model_dump(mode="json"),
        model_name=agent.MODEL_NAME,
        input_tokens=0,
        output_tokens=0,
        started_at=now,
        completed_at=now,
    )
    session.add(row)
    await session.flush()
    return row


async def _event(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    event_type: str,
    event_status: str,
    message: str,
    progress: int,
    payload: dict[str, Any] | None = None,
) -> None:
    session.add(
        WorkflowEventETL(
            workflow_run_id=workflow.id,
            event_type=event_type,
            stage_name="VALIDATION",
            status=event_status,
            progress_percentage=progress,
            message=message,
            event_payload=payload or {},
            actor_type="SYSTEM",
        )
    )


async def _latest_discovery(
    session: AsyncSession,
    mapping_id: int,
) -> dict[str, Any]:
    row = await session.scalar(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.etl_object_id == mapping_id,
            AgentResponseETL.agent_name == "DISCOVERY_AGENT",
            AgentResponseETL.status == "COMPLETED",
        )
        .order_by(AgentResponseETL.id.desc())
        .limit(1)
    )
    return row.response_payload if row else {}


async def _artifacts(
    session: AsyncSession,
    conversion_workflow_id: str,
    mapping_id: int,
) -> dict[str, GeneratedArtifactETL]:
    conversion_workflow = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == conversion_workflow_id,
            WorkflowRunETL.job_type == "CONVERSION",
        )
    )
    if conversion_workflow is None:
        raise RuntimeError("Conversion workflow was not found.")

    rows = list(
        (
            await session.scalars(
                select(GeneratedArtifactETL)
                .where(
                    GeneratedArtifactETL.workflow_run_id
                    == conversion_workflow.id,
                    GeneratedArtifactETL.etl_object_id == mapping_id,
                )
                .order_by(GeneratedArtifactETL.id.desc())
            )
        ).all()
    )

    selected: dict[str, GeneratedArtifactETL] = {}
    for row in rows:
        selected.setdefault(row.artifact_type, row)
    return selected


async def _persist_static_results(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    agent_response: AgentResponseETL,
    static_result: Any,
) -> None:
    for check in static_result.checks:
        test_id = STATIC_TEST_IDS.get(check.check_name)
        if test_id is None:
            continue

        await upsert_test_case(
            session,
            workflow_run_id=workflow.id,
            etl_object_id=mapping.id,
            agent_response_id=agent_response.id,
            test_id=test_id,
            test_name=check.check_name.replace("_", " ").title(),
            category="STATIC_VALIDATION",
            status="PASSED" if check.passed else "FAILED",
            severity=check.severity,
            expected_result="The static validation check passes.",
            actual_result="PASSED" if check.passed else "FAILED",
            details=check.details,
            evidence_payload={"check_name": check.check_name},
        )


async def _persist_unit_results(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    agent_response: AgentResponseETL,
    unit_result: Any,
) -> None:
    for test_case in unit_result.test_cases:
        await upsert_test_case(
            session,
            workflow_run_id=workflow.id,
            etl_object_id=mapping.id,
            agent_response_id=agent_response.id,
            test_id=test_case.test_id,
            test_name=test_case.title,
            category=test_case.category,
            status=test_case.status,
            severity="HIGH",
            expected_result="The generated pytest test passes.",
            actual_result=test_case.status,
            details=test_case.details,
            evidence_payload=test_case.evidence,
            duration_seconds=test_case.duration_seconds,
        )


async def _persist_parity_results(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    agent_response: AgentResponseETL,
    parity_result: Any,
    minimum: float,
) -> None:
    parity_values = [
        ("PARITY-001", "Source coverage", parity_result.source_coverage),
        ("PARITY-002", "Target coverage", parity_result.target_coverage),
        (
            "PARITY-003",
            "Target-field coverage",
            parity_result.target_field_coverage,
        ),
        (
            "PARITY-004",
            "Business-rule coverage",
            parity_result.business_rule_coverage,
        ),
        ("PARITY-005", "Lineage coverage", parity_result.lineage_coverage),
        (
            "PARITY-006",
            "Precision and scale coverage",
            parity_result.precision_scale_coverage,
        ),
        (
            "PARITY-007",
            "Overall functional parity",
            parity_result.overall_functional_parity,
        ),
    ]

    for test_id, test_name, value in parity_values:
        threshold = minimum if test_id == "PARITY-007" else 1.0
        passed = value >= threshold

        await upsert_test_case(
            session,
            workflow_run_id=workflow.id,
            etl_object_id=mapping.id,
            agent_response_id=agent_response.id,
            test_id=test_id,
            test_name=test_name,
            category="FUNCTIONAL_PARITY",
            status="PASSED" if passed else "FAILED",
            severity="CRITICAL" if test_id == "PARITY-007" else "HIGH",
            expected_result=f"Coverage is at least {threshold:.2f}.",
            actual_result=f"{value:.4f}",
            details=f"Measured coverage: {value:.4f}.",
            evidence_payload={
                "coverage": value,
                "threshold": threshold,
            },
        )


async def _process(workflow_id: int) -> None:
    """Execute the detailed Validation flow for one workflow."""
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow is None:
            raise RuntimeError("Validation workflow was not found.")

        mapping_ids = workflow.scope_payload.get("etl_object_ids", [])
        conversion_workflow_id = workflow.scope_payload[
            "conversion_workflow_id"
        ]
        minimum = float(
            workflow.scope_payload.get(
                "minimum_functional_parity",
                settings.minimum_functional_parity,
            )
        )

        mappings = list(
            (
                await session.scalars(
                    select(ETLObjectETL)
                    .where(
                        ETLObjectETL.id.in_(mapping_ids),
                        ETLObjectETL.repository_id == workflow.repository_id,
                    )
                    .order_by(
                        ETLObjectETL.migration_wave,
                        ETLObjectETL.id,
                    )
                )
            ).all()
        )

        if not mappings:
            raise RuntimeError("No mappings were found for validation.")

        all_passed = True

        for index, mapping in enumerate(mappings, start=1):
            generated = await _artifacts(
                session,
                conversion_workflow_id,
                mapping.id,
            )
            paths = {
                artifact_type: Path(row.storage_path)
                for artifact_type, row in generated.items()
            }

            mapping_model = CanonicalMapping.model_validate(
                mapping.source_definition
            )
            discovery = await _latest_discovery(session, mapping.id)

            plan_agent = TestPlanAgent()
            plan_result = plan_agent.run(mapping_model, discovery)
            await _audit(
                session,
                workflow,
                mapping,
                plan_agent,
                plan_result,
            )
            await replace_planned_test_cases(
                session,
                workflow.id,
                mapping.id,
                plan_result.test_cases,
            )

            static_agent = StaticValidationAgent()
            static_result = static_agent.run(paths)
            static_audit = await _audit(
                session,
                workflow,
                mapping,
                static_agent,
                static_result,
            )
            await _persist_static_results(
                session,
                workflow,
                mapping,
                static_audit,
                static_result,
            )

            unit_agent = UnitTestExecutionAgent()
            if static_result.safe_to_execute_tests:
                unit_result = unit_agent.run(
                    paths["UNIT_TEST"],
                    settings.validation_test_timeout_seconds,
                )
            else:
                unit_result = unit_agent.skipped(
                    "Static validation failed; execution was blocked safely."
                )

            unit_audit = await _audit(
                session,
                workflow,
                mapping,
                unit_agent,
                unit_result,
            )
            await _persist_unit_results(
                session,
                workflow,
                mapping,
                unit_audit,
                unit_result,
            )

            code_path = paths.get("PYSPARK_CODE")
            code = (
                code_path.read_text(encoding="utf-8")
                if code_path and code_path.is_file()
                else ""
            )

            parity_agent = FunctionalParityAgent()
            parity_result = parity_agent.run(
                mapping_model,
                discovery,
                mapping.lineage or {},
                code,
                minimum,
            )
            parity_audit = await _audit(
                session,
                workflow,
                mapping,
                parity_agent,
                parity_result,
            )
            await _persist_parity_results(
                session,
                workflow,
                mapping,
                parity_audit,
                parity_result,
                minimum,
            )

            passed = (
                static_result.status == "PASSED"
                and unit_result.status == "PASSED"
                and parity_result.status == "PASSED"
            )
            all_passed = all_passed and passed
            mapping.migration_status = (
                "VALIDATED" if passed else "VALIDATION_FAILED"
            )

            for artifact in generated.values():
                artifact.validation_status = (
                    "VALIDATED" if passed else "FAILED"
                )
                artifact.is_deployable = passed

            progress = int(index / len(mappings) * 90)
            await _event(
                session,
                workflow,
                "MAPPING_VALIDATION_COMPLETED",
                "COMPLETED" if passed else "FAILED",
                (
                    f"Validation {'passed' if passed else 'failed'} "
                    f"for {mapping.object_name}."
                ),
                progress,
                {
                    "etl_object_id": mapping.id,
                    "passed": passed,
                    "planned_test_count": plan_result.test_count,
                },
            )
            await session.commit()

        workflow.current_stage = "VALIDATION_COMPLETED"
        workflow.job_status = "COMPLETED" if all_passed else "FAILED"
        workflow.overall_status = "COMPLETED" if all_passed else "FAILED"
        workflow.failure_stage = None if all_passed else "VALIDATION"
        workflow.failure_reason = (
            None
            if all_passed
            else "One or more mappings failed validation."
        )
        workflow.completed_at = datetime.now(timezone.utc)

        await _event(
            session,
            workflow,
            "VALIDATION_COMPLETED",
            workflow.overall_status,
            "Validation workflow completed.",
            100,
            {"all_passed": all_passed},
        )
        await session.commit()


async def _fail(workflow_id: int, exc: Exception) -> None:
    """Mark an unexpected Validation processing error."""
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow is None:
            return

        workflow.current_stage = "VALIDATION"
        workflow.job_status = "FAILED"
        workflow.overall_status = "FAILED"
        workflow.failure_stage = "VALIDATION"
        workflow.failure_reason = str(exc)[:4000]
        workflow.completed_at = datetime.now(timezone.utc)

        await _event(
            session,
            workflow,
            "VALIDATION_FAILED",
            "FAILED",
            "Validation workflow failed.",
            100,
            {
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            },
        )
        await session.commit()


async def run_validation_worker() -> None:
    """Continuously poll for queued Validation workflows."""
    while True:
        workflow_id = await _claim()
        if workflow_id is None:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        try:
            await _process(workflow_id)
        except Exception as exc:
            await _fail(workflow_id, exc)


if __name__ == "__main__":
    asyncio.run(run_validation_worker())
