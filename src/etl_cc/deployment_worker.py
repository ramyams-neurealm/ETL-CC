"""Queued Git deployment worker."""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import select
from etl_cc.agents.deployment_agent import DeploymentArtifact, GitDeploymentAgent, GitDeploymentContext
from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.models import AgentResponseETL, ETLObjectETL, GeneratedArtifactETL, RepositoryETL, WorkflowEventETL, WorkflowRunETL
from etl_cc.security import credential_cipher

async def _claim():
    async with SessionFactory() as session:
        row=await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.job_type=="DEPLOYMENT",WorkflowRunETL.job_status=="QUEUED").order_by(WorkflowRunETL.priority,WorkflowRunETL.id).with_for_update(skip_locked=True).limit(1))
        if row is None: return None
        row.job_status="RUNNING"; row.overall_status="RUNNING"; row.current_stage="DEPLOYMENT"; row.started_at=datetime.now(timezone.utc); row.attempt_count+=1
        session.add(WorkflowEventETL(workflow_run_id=row.id,event_type="DEPLOYMENT_STARTED",stage_name="DEPLOYMENT",status="RUNNING",progress_percentage=5,message="Git deployment started.",event_payload={},actor_type="SYSTEM")); await session.commit(); return row.id

async def _process(run_id):
    async with SessionFactory() as session:
        workflow=await session.get(WorkflowRunETL,run_id); scope=workflow.scope_payload; validation=await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.workflow_id==scope["validation_workflow_id"],WorkflowRunETL.job_type=="VALIDATION"))
        target_repo=await session.get(RepositoryETL,scope["target_repository_id"]); token=credential_cipher.decrypt(target_repo.credential_ciphertext) if target_repo.credential_ciphertext else None
        remote_url=target_repo.connection_config.get("repository_url"); base_branch=scope.get("base_branch") or target_repo.connection_config.get("branch") or "main"
        validation_agent=await session.scalar(select(AgentResponseETL).where(AgentResponseETL.workflow_run_id==validation.id,AgentResponseETL.etl_object_id==workflow.etl_object_id,AgentResponseETL.agent_name=="VALIDATION_AGENT").order_by(AgentResponseETL.id.desc()).limit(1))
        mapping=await session.get(ETLObjectETL,workflow.etl_object_id)
        artifacts=list((await session.scalars(select(GeneratedArtifactETL).where(GeneratedArtifactETL.etl_object_id==mapping.id,GeneratedArtifactETL.validation_status=="VALIDATED",GeneratedArtifactETL.is_deployable.is_(True)).order_by(GeneratedArtifactETL.artifact_version.desc(),GeneratedArtifactETL.id.desc()))).all())
        evidence=dict(validation_agent.response_payload or {})
        context=GitDeploymentContext(workflow_id=workflow.workflow_id,repository_id=workflow.repository_id,mapping_id=mapping.id,mapping_name=mapping.object_name,validation_workflow_id=validation.workflow_id,remote_url=remote_url,base_branch=base_branch,branch_name=scope["branch_name"],repository_path=scope.get("repository_path") or settings.git_deployment_repository_path,commit_message=scope["commit_message"],create_pull_request=bool(scope.get("create_pull_request")),pull_request_title=scope.get("pull_request_title"),pull_request_body=scope.get("pull_request_body"),access_token=token,artifacts=[DeploymentArtifact(artifact_id=a.id,artifact_type=a.artifact_type,file_name=a.file_name,storage_path=a.storage_path,content_hash=a.content_hash) for a in artifacts],validation_evidence=evidence)
    result=await asyncio.to_thread(GitDeploymentAgent().run,context)
    async with SessionFactory() as session:
        workflow=await session.get(WorkflowRunETL,run_id); mapping=await session.get(ETLObjectETL,workflow.etl_object_id)
        response=AgentResponseETL(workflow_run_id=run_id,etl_object_id=mapping.id,agent_name=GitDeploymentAgent.AGENT_NAME,agent_version=GitDeploymentAgent.AGENT_VERSION,stage_name=GitDeploymentAgent.STAGE_NAME,status="COMPLETED" if result.status=="DEPLOYED" else "FAILED",response_payload=result.model_dump(mode="json"),model_name=GitDeploymentAgent.MODEL_NAME,error_message=result.failure_reason,started_at=workflow.started_at,completed_at=datetime.now(timezone.utc)); session.add(response)
        if result.status=="DEPLOYED":
            workflow.current_stage="DEPLOYMENT_COMPLETED"; workflow.overall_status="COMPLETED"; workflow.job_status="COMPLETED"; mapping.migration_status="DEPLOYED"
            for artifact in (await session.scalars(select(GeneratedArtifactETL).where(GeneratedArtifactETL.etl_object_id==mapping.id))).all():
                artifact.git_path=next((p for p in result.deployed_paths if p.endswith(Path(artifact.file_name).name)),artifact.git_path)
            event_type="DEPLOYMENT_COMPLETED"; message="Validated artifacts were deployed to Git."; progress=100
        else:
            workflow.current_stage="DEPLOYMENT_COMPLETED"; workflow.overall_status="FAILED"; workflow.job_status="FAILED"; workflow.failure_stage="DEPLOYMENT"; workflow.failure_reason=result.failure_reason; mapping.migration_status="DEPLOYMENT_FAILED"; event_type="DEPLOYMENT_FAILED"; message=result.failure_reason or "Git deployment failed."; progress=100
        workflow.completed_at=datetime.now(timezone.utc); session.add(WorkflowEventETL(workflow_run_id=workflow.id,agent_response_id=response.id,event_type=event_type,stage_name="DEPLOYMENT",status=workflow.job_status,progress_percentage=progress,message=message,event_payload=result.model_dump(mode="json"),actor_type="SYSTEM")); await session.commit()

async def main():
    while True:
        run_id=await _claim()
        if run_id is None: await asyncio.sleep(settings.worker_poll_seconds); continue
        try: await _process(run_id)
        except Exception as exc:
            async with SessionFactory() as session:
                row=await session.get(WorkflowRunETL,run_id); row.current_stage="DEPLOYMENT_COMPLETED"; row.overall_status="FAILED"; row.job_status="FAILED"; row.failure_stage="DEPLOYMENT"; row.failure_reason=str(exc)[:4000]; row.completed_at=datetime.now(timezone.utc); await session.commit()
if __name__=="__main__": asyncio.run(main())

