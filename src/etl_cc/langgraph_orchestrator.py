"""LangGraph-only runtime orchestrator for NeuFlow Migration.

The API creates a parent MIGRATION and queued CONVERSION child. This service is
then the only queue consumer and the only runtime owner of stage sequencing.
Stage implementations are imported from non-polling service modules.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy import and_, or_, select

from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.logging_config import configure_logging, log_event, log_exception
from etl_cc.models import WorkflowRunETL
from etl_cc import conversion_service, validation_service, deployment_service
from etl_cc.agents.unit_test_specification import UnitTestSpecificationValidator

logger = configure_logging("LANGGRAPH_ORCHESTRATOR")


class MigrationState(TypedDict, total=False):
    migration_id: str
    migration_run_id: int
    conversion_workflow_id: str
    conversion_run_id: int
    validation_workflow_id: str | None
    validation_run_id: int | None
    node: str
    conversion_status: str
    validation_status: str
    deployment_status: str
    deployable: bool
    deployment_queued: bool
    failed: bool
    failure_stage: str | None
    failure_reason: str | None


async def _workflow(workflow_id: str) -> WorkflowRunETL | None:
    async with SessionFactory() as session:
        return await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.workflow_id == workflow_id))


async def _child(migration_id: str, job_type: str) -> WorkflowRunETL | None:
    async with SessionFactory() as session:
        return await session.scalar(
            select(WorkflowRunETL)
            .where(WorkflowRunETL.batch_id == migration_id, WorkflowRunETL.job_type == job_type)
            .order_by(WorkflowRunETL.id.desc())
        )


def _log(state: MigrationState, event: str, **payload: Any) -> None:
    log_event(logger, event, migration_id=state.get("migration_id"), node=state.get("node"), **payload)


async def conversion_node(state: MigrationState) -> dict[str, Any]:
    _log(state, "LANGGRAPH_NODE_STARTED", node_name="CONVERSION_AND_CRITIQUE")
    try:
        await conversion_service.execute_conversion_workflow(int(state["conversion_run_id"]))
        row = await _workflow(state["conversion_workflow_id"])
        if row is None or row.overall_status != "COMPLETED":
            return {"node":"CONVERSION_AND_CRITIQUE","conversion_status":"FAILED","failed":True,"failure_stage":"CONVERSION","failure_reason":row.failure_reason if row else "Conversion workflow was not found."}
        _log(state, "LANGGRAPH_NODE_COMPLETED", node_name="CONVERSION_AND_CRITIQUE")
        return {"node":"CONVERSION_AND_CRITIQUE","conversion_status":"COMPLETED"}
    except Exception as exc:
        await conversion_service.fail_conversion_workflow(int(state["conversion_run_id"]), exc)
        log_exception(logger, "LANGGRAPH_CONVERSION_FAILED", exc, migration_id=state.get("migration_id"))
        return {"node":"CONVERSION_AND_CRITIQUE","conversion_status":"FAILED","failed":True,"failure_stage":"CONVERSION","failure_reason":str(exc)}


async def validation_node(state: MigrationState) -> dict[str, Any]:
    _log(state, "LANGGRAPH_NODE_STARTED", node_name="VALIDATION")
    child = await _child(state["migration_id"], "VALIDATION")
    if child is None:
        return {"node":"VALIDATION","validation_status":"FAILED","deployable":False,"failed":True,"failure_stage":"VALIDATION","failure_reason":"Validation workflow was not created."}
    try:
        await validation_service.execute_validation_workflow(child.id)
        row = await _workflow(child.workflow_id)
        passed = bool(row and row.overall_status == "COMPLETED")
        # Validation decides whether Git deployment is configured and queued.
        migration = await _workflow(state["migration_id"])
        deployment_queued = bool(passed and migration and migration.current_stage == "DEPLOYMENT_QUEUED")
        _log(state, "LANGGRAPH_NODE_COMPLETED", node_name="VALIDATION", status=row.overall_status if row else "FAILED", deployment_queued=deployment_queued)
        return {"node":"VALIDATION","validation_workflow_id":child.workflow_id,"validation_run_id":child.id,"validation_status":row.overall_status if row else "FAILED","deployable":passed,"deployment_queued":deployment_queued,"failed":not passed,"failure_stage":None if passed else "VALIDATION","failure_reason":None if passed else (row.failure_reason if row else "Validation workflow was not found.")}
    except Exception as exc:
        await validation_service.fail_validation_workflow(child.id, exc)
        log_exception(logger, "LANGGRAPH_VALIDATION_FAILED", exc, migration_id=state.get("migration_id"))
        return {"node":"VALIDATION","validation_status":"FAILED","deployable":False,"failed":True,"failure_stage":"VALIDATION","failure_reason":str(exc)}


async def deployment_node(state: MigrationState) -> dict[str, Any]:
    _log(state, "LANGGRAPH_NODE_STARTED", node_name="DEPLOYMENT")
    try:
        await deployment_service.execute_deployment_workflow(int(state["migration_run_id"]))
        row = await _workflow(state["migration_id"])
        passed = bool(row and row.current_stage == "DEPLOYMENT_COMPLETED" and row.overall_status == "COMPLETED")
        _log(state, "LANGGRAPH_NODE_COMPLETED", node_name="DEPLOYMENT", status=row.overall_status if row else "FAILED")
        return {"node":"DEPLOYMENT","deployment_status":"COMPLETED" if passed else "FAILED","failed":not passed,"failure_stage":None if passed else "DEPLOYMENT","failure_reason":None if passed else (row.failure_reason if row else "Migration workflow was not found.")}
    except Exception as exc:
        await deployment_service.fail_deployment_workflow(int(state["migration_run_id"]), exc)
        log_exception(logger, "LANGGRAPH_DEPLOYMENT_FAILED", exc, migration_id=state.get("migration_id"))
        return {"node":"DEPLOYMENT","deployment_status":"FAILED","failed":True,"failure_stage":"DEPLOYMENT","failure_reason":str(exc)}


async def complete_node(state: MigrationState) -> dict[str, Any]:
    _log(state, "LANGGRAPH_MIGRATION_COMPLETED")
    return {"node":"COMPLETE"}


async def failed_node(state: MigrationState) -> dict[str, Any]:
    _log(state, "LANGGRAPH_MIGRATION_FAILED", failure_stage=state.get("failure_stage"), failure_reason=state.get("failure_reason"))
    return {"node":"FAILED"}


def after_conversion(state: MigrationState) -> Literal["validation","failed"]:
    return "failed" if state.get("failed") else "validation"

def after_validation(state: MigrationState) -> Literal["deployment","complete","failed"]:
    if state.get("failed") or not state.get("deployable"):
        return "failed"
    # Passed without Git configuration: Migration is already READY_FOR_DEPLOYMENT.
    return "deployment" if state.get("deployment_queued") else "complete"

def after_deployment(state: MigrationState) -> Literal["complete","failed"]:
    return "complete" if state.get("deployment_status") == "COMPLETED" and not state.get("failed") else "failed"


def build_graph():
    graph=StateGraph(MigrationState)
    graph.add_node("conversion",conversion_node); graph.add_node("validation",validation_node); graph.add_node("deployment",deployment_node); graph.add_node("complete",complete_node); graph.add_node("failed",failed_node)
    graph.add_edge(START,"conversion")
    graph.add_conditional_edges("conversion",after_conversion,{"validation":"validation","failed":"failed"})
    graph.add_conditional_edges("validation",after_validation,{"deployment":"deployment","complete":"complete","failed":"failed"})
    graph.add_conditional_edges("deployment",after_deployment,{"complete":"complete","failed":"failed"})
    graph.add_edge("complete",END); graph.add_edge("failed",END)
    return graph.compile()

MIGRATION_GRAPH=build_graph()


def _mark_claimed(row: WorkflowRunETL) -> None:
    row.job_status="RUNNING"; row.overall_status="RUNNING"; row.attempt_count+=1
    row.started_at=row.started_at or datetime.now(timezone.utc)


async def claim_migration() -> MigrationState | dict[str, Any] | None:
    """Claim a queued Conversion; standalone conversions return a plain job dict."""
    async with SessionFactory() as session:
        async with session.begin():
            conversion=await session.scalar(
                select(WorkflowRunETL)
                .where(WorkflowRunETL.job_type=="CONVERSION",WorkflowRunETL.job_status=="QUEUED")
                .order_by(WorkflowRunETL.priority,WorkflowRunETL.created_at)
                .with_for_update(skip_locked=True).limit(1)
            )
            if conversion is None:return None
            _mark_claimed(conversion)
            if not conversion.batch_id:
                return {"job_type":"CONVERSION","run_id":conversion.id}
            migration=await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.workflow_id==conversion.batch_id,WorkflowRunETL.job_type=="MIGRATION"))
            if migration is None:
                conversion.job_status="FAILED"; conversion.overall_status="FAILED"; conversion.failure_stage="ORCHESTRATION"; conversion.failure_reason="Parent Migration was not found."
                return None
            return MigrationState(migration_id=migration.workflow_id,migration_run_id=migration.id,conversion_workflow_id=conversion.workflow_id,conversion_run_id=conversion.id,node="CLAIMED",failed=False,deployable=False,deployment_queued=False)


async def claim_standalone_job() -> dict[str, Any] | None:
    """Claim an API-queued Validation or Deployment.

    Automatic Validation children are run inside the Migration graph, so only
    validations created through POST /validations (trigger=API) are claimed.
    """
    async with SessionFactory() as session:
        async with session.begin():
            row=await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_status=="QUEUED",
                    or_(
                        and_(WorkflowRunETL.job_type=="VALIDATION",WorkflowRunETL.scope_payload["trigger"].astext=="API"),
                        WorkflowRunETL.job_type=="DEPLOYMENT",
                    ),
                )
                .order_by(WorkflowRunETL.priority,WorkflowRunETL.created_at)
                .with_for_update(skip_locked=True).limit(1)
            )
            if row is None:return None
            _mark_claimed(row)
            return {"job_type":row.job_type,"run_id":row.id,"workflow_id":row.workflow_id,"batch_id":row.batch_id}


async def run_standalone_job(job: dict[str, Any]) -> None:
    """Run one stage outside the Migration graph."""
    run_id=int(job["run_id"])
    if job["job_type"]=="CONVERSION":
        try: await conversion_service.execute_conversion_workflow(run_id)
        except Exception as exc:
            await conversion_service.fail_conversion_workflow(run_id,exc); log_exception(logger,"STANDALONE_CONVERSION_FAILED",exc,run_id=run_id)
    elif job["job_type"]=="VALIDATION":
        try: await validation_service.execute_validation_workflow(run_id)
        except Exception as exc:
            await validation_service.fail_validation_workflow(run_id,exc); log_exception(logger,"STANDALONE_VALIDATION_FAILED",exc,run_id=run_id); return
        # A passing re-validation of a Migration may have queued automatic deployment.
        migration=await _workflow(job["batch_id"]) if job.get("batch_id") else None
        if migration is not None and migration.current_stage=="DEPLOYMENT_QUEUED" and migration.job_status=="QUEUED":
            try: await deployment_service.execute_deployment_workflow(migration.id)
            except Exception as exc:
                await deployment_service.fail_deployment_workflow(migration.id,exc); log_exception(logger,"STANDALONE_DEPLOYMENT_FAILED",exc,migration_id=migration.workflow_id)
    elif job["job_type"]=="DEPLOYMENT":
        try: await deployment_service.execute_manual_deployment_workflow(run_id)
        except Exception as exc:
            await deployment_service.fail_manual_deployment_workflow(run_id,exc); log_exception(logger,"MANUAL_DEPLOYMENT_FAILED",exc,run_id=run_id)


async def run_orchestrator() -> None:
    log_event(
        logger,
        "LANGGRAPH_ORCHESTRATOR_STARTED",
        graph="MIGRATION_GRAPH",
        graph_version="2.2.0",
        unit_test_specification_version=UnitTestSpecificationValidator.VERSION,
    )
    while True:
        state=await claim_migration()
        if state is not None and "migration_id" not in state:
            await run_standalone_job(state); continue
        if state is None:
            job=await claim_standalone_job()
            if job is None:
                await asyncio.sleep(settings.worker_poll_seconds); continue
            await run_standalone_job(job); continue
        try:
            await MIGRATION_GRAPH.ainvoke(state,config={"configurable":{"thread_id":state["migration_id"]}})
        except Exception as exc:
            log_exception(logger,"LANGGRAPH_UNHANDLED_FAILURE",exc,migration_id=state.get("migration_id"))
        await asyncio.sleep(0)

if __name__=="__main__":asyncio.run(run_orchestrator())
