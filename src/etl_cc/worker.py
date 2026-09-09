"""Worker for source discovery, per-mapping agents, and dependency planning."""

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.discovery_agent import DiscoveryAgent
from etl_cc.agents.lineage_agent import LineageAgent
from etl_cc.config import settings
from etl_cc.connectors import GitHubSource, MappingSource, PowerCenterSource
from etl_cc.database import SessionFactory
from etl_cc.dependency_planner import DependencyPlanner
from etl_cc.informatica_parser import InformaticaXMLParser
from etl_cc.models import AgentResponseETL, CanonicalMapping, ETLObjectETL, RepositoryETL, WorkflowEventETL, WorkflowRunETL
from etl_cc.security import credential_cipher
from etl_cc.source_store import load_manifest


def _hash(value: dict[str, Any]) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _event(session: AsyncSession, workflow_id: int, event_type: str, status: str, message: str, progress: int, payload: dict | None = None, stage: str = "DISCOVERY", agent_response_id: int | None = None) -> None:
    session.add(WorkflowEventETL(workflow_run_id=workflow_id, agent_response_id=agent_response_id, event_type=event_type, stage_name=stage, status=status, progress_percentage=progress, message=message, event_payload=payload or {}, actor_type="SYSTEM"))
    await session.flush()


async def _claim() -> int | None:
    async with SessionFactory() as session:
        async with session.begin():
            row = await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.job_status == "QUEUED", WorkflowRunETL.job_type == "DISCOVERY").order_by(WorkflowRunETL.priority, WorkflowRunETL.created_at).with_for_update(skip_locked=True).limit(1))
            if row is None:
                return None
            row.job_status = "RUNNING"
            row.overall_status = "RUNNING"
            row.current_stage = "DISCOVERY"
            row.started_at = datetime.now(timezone.utc)
            row.attempt_count += 1
            return row.id


async def _source(repository: RepositoryETL, workflow: WorkflowRunETL) -> MappingSource | None:
    manifest = load_manifest(workflow.scope_payload["source_id"])
    credential = credential_cipher.decrypt(repository.credential_ciphertext) if repository.credential_ciphertext else None
    directory = settings.project_root / "runtime_sources" / repository.connection_type.lower() / manifest["source_id"]
    if repository.connection_type == "POWERCENTER":
        return PowerCenterSource(manifest["config"], credential or "")
    if repository.connection_type == "GITHUB":
        return GitHubSource(manifest["config"], credential, directory)
    if repository.connection_type == "XML_UPLOAD":
        return None
    raise ValueError(f"Unsupported connection type: {repository.connection_type}")


async def _load_mappings(repository: RepositoryETL, workflow: WorkflowRunETL) -> list[CanonicalMapping]:
    selected = workflow.scope_payload.get("selected_mapping_keys") or None
    manifest = load_manifest(workflow.scope_payload["source_id"])
    if repository.connection_type == "XML_UPLOAD":
        path = Path(manifest["config"]["stored_file_path"])
        return InformaticaXMLParser().parse_selected(path, set(selected) if selected else None, "INFORMATICA_XML_UPLOAD", path.name)
    source = await _source(repository, workflow)
    if source is None:
        raise ValueError("Source adapter was not created.")
    return await source.get_mapping_details(selected)


async def _upsert(session: AsyncSession, repository: RepositoryETL, mapping: CanonicalMapping) -> ETLObjectETL:
    canonical = mapping.model_dump(mode="json")
    row = await session.scalar(select(ETLObjectETL).where(ETLObjectETL.repository_id == repository.id, ETLObjectETL.source_object_key == mapping.source_object_key))
    if row is None:
        row = ETLObjectETL(repository_id=repository.id, source_object_key=mapping.source_object_key, object_name=mapping.mapping_name, object_type="MAPPING", content_hash=_hash(canonical))
        session.add(row)
    row.object_name = mapping.mapping_name
    row.folder_path = mapping.folder_name
    row.source_definition = canonical
    row.parameters = [item.model_dump(mode="json") for item in mapping.parameters]
    row.transformation_count = len(mapping.transformations)
    row.content_hash = _hash(canonical)
    row.discovery_status = "DISCOVERED"
    await session.flush()
    return row


async def _attempt(session: AsyncSession, workflow_id: int, etl_object_id: int | None, agent_name: str, stage: str) -> int:
    conditions = [AgentResponseETL.workflow_run_id == workflow_id, AgentResponseETL.agent_name == agent_name, AgentResponseETL.stage_name == stage]
    conditions.append(AgentResponseETL.etl_object_id.is_(None) if etl_object_id is None else AgentResponseETL.etl_object_id == etl_object_id)
    value = await session.scalar(select(func.coalesce(func.max(AgentResponseETL.attempt_number), 0)).where(*conditions))
    return int(value or 0) + 1


async def _run_mapping_agent(session: AsyncSession, workflow: WorkflowRunETL, repository: RepositoryETL, row: ETLObjectETL, mapping: CanonicalMapping, agent, progress: int):
    attempt = await _attempt(session, workflow.id, row.id, agent.AGENT_NAME, agent.STAGE_NAME)
    audit = AgentResponseETL(workflow_run_id=workflow.id, etl_object_id=row.id, agent_name=agent.AGENT_NAME, agent_version=agent.AGENT_VERSION, stage_name=agent.STAGE_NAME, attempt_number=attempt, status="RUNNING", request_payload={"repository_id": repository.id, "workflow_id": workflow.workflow_id, "etl_object_id": row.id, "canonical_mapping": mapping.model_dump(mode="json")}, response_payload={}, model_name=getattr(agent, "MODEL_NAME", "DETERMINISTIC"), input_tokens=0, output_tokens=0, started_at=datetime.now(timezone.utc))
    session.add(audit)
    await session.flush()
    await _event(session, workflow.id, f"{agent.AGENT_NAME}_STARTED", "RUNNING", f"{agent.AGENT_NAME} started for {mapping.mapping_name}.", progress, {"etl_object_id": row.id, "mapping_name": mapping.mapping_name}, agent.STAGE_NAME, audit.id)
    await session.commit()
    try:
        result = await agent.run(mapping)
        audit.status = "COMPLETED"
        audit.response_payload = result.model_dump(mode="json")
        audit.completed_at = datetime.now(timezone.utc)
        await _event(session, workflow.id, f"{agent.AGENT_NAME}_COMPLETED", "COMPLETED", f"{agent.AGENT_NAME} completed for {mapping.mapping_name}.", progress, {"etl_object_id": row.id, "mapping_name": mapping.mapping_name, "agent_response_id": audit.id}, agent.STAGE_NAME, audit.id)
        await session.commit()
        return result
    except Exception as exc:
        audit.status = "FAILED"
        audit.error_message = str(exc)[:4000]
        audit.completed_at = datetime.now(timezone.utc)
        await _event(session, workflow.id, f"{agent.AGENT_NAME}_FAILED", "FAILED", f"{agent.AGENT_NAME} failed for {mapping.mapping_name}.", progress, {"etl_object_id": row.id}, agent.STAGE_NAME, audit.id)
        await session.commit()
        raise


async def _run_planner(session: AsyncSession, workflow: WorkflowRunETL, repository: RepositoryETL, mappings: list[CanonicalMapping], rows: dict[str, ETLObjectETL]):
    planner = DependencyPlanner()
    attempt = await _attempt(session, workflow.id, None, planner.AGENT_NAME, planner.STAGE_NAME)
    audit = AgentResponseETL(workflow_run_id=workflow.id, etl_object_id=None, agent_name=planner.AGENT_NAME, agent_version=planner.AGENT_VERSION, stage_name=planner.STAGE_NAME, attempt_number=attempt, status="RUNNING", request_payload={"repository_id": repository.id, "workflow_id": workflow.workflow_id, "canonical_mappings": [item.model_dump(mode="json") for item in mappings]}, response_payload={}, model_name=planner.MODEL_NAME, input_tokens=0, output_tokens=0, started_at=datetime.now(timezone.utc))
    session.add(audit)
    await session.flush()
    await _event(session, workflow.id, "DEPENDENCY_PLANNING_STARTED", "RUNNING", "Repository dependency planning started.", 92, {"mapping_count": len(mappings)}, planner.STAGE_NAME, audit.id)
    await session.commit()
    try:
        plan = await planner.run(mappings)
        audit.status = "COMPLETED"
        audit.response_payload = plan.model_dump(mode="json")
        audit.completed_at = datetime.now(timezone.utc)
        by_downstream: dict[str, list[dict]] = {}
        for dependency in plan.dependencies:
            by_downstream.setdefault(dependency.downstream_object_key, []).append(dependency.model_dump(mode="json"))
        for key, map_plan in plan.mapping_plans.items():
            row = rows[key]
            row.dependencies = {"items": by_downstream.get(key, []), "upstream_mapping_keys": map_plan.upstream_mapping_keys, "downstream_mapping_keys": map_plan.downstream_mapping_keys}
            row.has_dependency_cycle = map_plan.has_dependency_cycle
            row.migration_wave = map_plan.migration_wave
        await _event(session, workflow.id, "DEPENDENCY_PLANNING_COMPLETED", "COMPLETED", "Repository dependency planning completed.", 98, {"dependency_count": len(plan.dependencies), "wave_count": len(plan.migration_waves), "has_dependency_cycle": plan.has_dependency_cycle}, planner.STAGE_NAME, audit.id)
        await session.commit()
        return plan
    except Exception as exc:
        audit.status = "FAILED"
        audit.error_message = str(exc)[:4000]
        audit.completed_at = datetime.now(timezone.utc)
        await session.commit()
        raise


async def _process(workflow_id: int) -> None:
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        repository = await session.get(RepositoryETL, workflow.repository_id) if workflow else None
        if workflow is None or repository is None:
            raise ValueError("Workflow or repository was not found.")
        await _event(session, workflow.id, "DISCOVERY_STARTED", "RUNNING", "Mapping discovery started.", 5, {"connection_type": repository.connection_type})
        await session.commit()
        mappings = await _load_mappings(repository, workflow)
        if not mappings:
            raise ValueError("No mappings matched the discovery scope.")
        rows: dict[str, ETLObjectETL] = {}
        total = len(mappings)
        for index, mapping in enumerate(mappings, 1):
            row = await _upsert(session, repository, mapping)
            rows[mapping.source_object_key] = row
            await _event(session, workflow.id, "MAPPING_DISCOVERED", "RUNNING", f"Discovered {mapping.mapping_name}.", 10 + int(index / total * 15), {"etl_object_id": row.id})
            await session.commit()
            discovery = await _run_mapping_agent(session, workflow, repository, row, mapping, DiscoveryAgent(), 25 + int(index / total * 20))
            row.business_purpose = discovery.business_purpose
            row.business_rules = [item.model_dump(mode="json") for item in discovery.business_rules]
            row.prerequisites = [item.model_dump(mode="json") for item in discovery.prerequisites]
            row.complexity = discovery.complexity
            row.complexity_score = discovery.complexity_score
            definition = dict(row.source_definition or {})
            definition["discovery_analysis"] = discovery.model_dump(mode="json")
            row.source_definition = definition
            await session.commit()
            lineage = await _run_mapping_agent(session, workflow, repository, row, mapping, LineageAgent(), 50 + int(index / total * 35))
            row.lineage = lineage.model_dump(mode="json")
            row.discovery_status = "ANALYZED"
            await session.commit()
        await _run_planner(session, workflow, repository, mappings, rows)
        repository.last_discovered_at = datetime.now(timezone.utc)
        workflow.job_status = "COMPLETED"
        workflow.overall_status = "COMPLETED"
        workflow.current_stage = "DEPENDENCY_PLANNING_COMPLETED"
        workflow.failure_stage = None
        workflow.failure_reason = None
        workflow.completed_at = datetime.now(timezone.utc)
        await _event(session, workflow.id, "DEPENDENCY_PLANNING_WORKFLOW_COMPLETED", "COMPLETED", "Discovery, lineage, and dependency planning completed.", 100, {"mapping_count": total}, "DEPENDENCY_PLANNING")
        await session.commit()


async def _fail(workflow_id: int, exc: Exception) -> None:
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow:
            workflow.job_status = "FAILED"
            workflow.overall_status = "FAILED"
            workflow.failure_stage = workflow.current_stage or "DISCOVERY"
            workflow.failure_reason = str(exc)[:4000]
            workflow.completed_at = datetime.now(timezone.utc)
            await _event(session, workflow.id, "WORKFLOW_FAILED", "FAILED", "Workflow failed.", 100, {"error_type": type(exc).__name__})
            await session.commit()


async def run_worker() -> None:
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
    asyncio.run(run_worker())
