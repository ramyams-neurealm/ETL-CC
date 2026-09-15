"""Deterministic Git deployment for validated ETL migration artifacts."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, Field


class DeploymentArtifact(BaseModel):
    artifact_id: int
    artifact_type: str
    file_name: str
    storage_path: str
    content_hash: str


class GitDeploymentContext(BaseModel):
    workflow_id: str
    repository_id: int
    mapping_id: int
    mapping_name: str
    validation_workflow_id: str
    remote_url: str
    base_branch: str = "main"
    branch_name: str
    repository_path: str = "migrations"
    commit_message: str
    create_pull_request: bool = False
    pull_request_title: str | None = None
    pull_request_body: str | None = None
    access_token: str | None = Field(default=None, exclude=True)
    artifacts: list[DeploymentArtifact]
    validation_evidence: dict[str, Any] = Field(default_factory=dict)


class DeploymentStage(BaseModel):
    stage_name: str
    status: Literal["COMPLETED", "FAILED", "SKIPPED"]
    details: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class GitDeploymentResult(BaseModel):
    status: Literal["DEPLOYED", "FAILED"]
    mapping_name: str
    branch_name: str
    commit_sha: str | None = None
    remote_url: str
    pull_request_url: str | None = None
    deployed_paths: list[str] = Field(default_factory=list)
    stages: list[DeploymentStage] = Field(default_factory=list)
    failure_reason: str | None = None


class GitDeploymentAgent:
    AGENT_NAME = "GIT_DEPLOYMENT_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "GIT_DEPLOYMENT"
    MODEL_NAME = "DETERMINISTIC_GIT"

    REQUIRED_ARTIFACTS = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
    TYPE_DIRECTORIES = {
        "PYSPARK_CODE": "src",
        "UNIT_TEST": "tests",
        "CONFIGURATION": "config",
    }

    def run(self, context: GitDeploymentContext) -> GitDeploymentResult:
        stages: list[DeploymentStage] = []
        try:
            self._validate_context(context)
            stages.append(DeploymentStage(stage_name="DEPLOYMENT_ELIGIBILITY", status="COMPLETED", details="Validation evidence authorizes Git deployment."))
            verified = self._verify_artifacts(context.artifacts)
            stages.append(DeploymentStage(stage_name="ARTIFACT_INTEGRITY", status="COMPLETED", details="Artifact files and SHA-256 hashes are valid.", evidence={"artifact_count": len(verified)}))
            with tempfile.TemporaryDirectory(prefix="neuflow-git-deploy-") as temp:
                worktree = Path(temp) / "repository"
                env, askpass = self._git_environment(context.access_token, Path(temp))
                self._clone(context.remote_url, context.base_branch, worktree, env)
                stages.append(DeploymentStage(stage_name="GIT_CLONE", status="COMPLETED", details=f"Cloned base branch {context.base_branch}."))
                self._run_git(["checkout", "-B", context.branch_name], worktree, env)
                deployed_paths = self._copy_artifacts(context, verified, worktree)
                self._write_manifest(context, verified, worktree, deployed_paths)
                stages.append(DeploymentStage(stage_name="DEPLOYMENT_PACKAGING", status="COMPLETED", details="Validated artifacts and deployment manifest were packaged.", evidence={"paths": deployed_paths}))
                self._run_git(["add", "--all"], worktree, env)
                changed = self._run_git(["status", "--porcelain"], worktree, env).stdout.strip()
                if not changed:
                    commit_sha = self._run_git(["rev-parse", "HEAD"], worktree, env).stdout.strip()
                    stages.append(DeploymentStage(stage_name="GIT_COMMIT", status="SKIPPED", details="Git content is already up to date."))
                else:
                    self._run_git(["-c", "user.name=NeuFlow", "-c", "user.email=neuflow@local", "commit", "-m", context.commit_message], worktree, env)
                    commit_sha = self._run_git(["rev-parse", "HEAD"], worktree, env).stdout.strip()
                    stages.append(DeploymentStage(stage_name="GIT_COMMIT", status="COMPLETED", details="Validated artifacts were committed.", evidence={"commit_sha": commit_sha}))
                self._run_git(["push", "--force-with-lease", "origin", f"HEAD:refs/heads/{context.branch_name}"], worktree, env)
                stages.append(DeploymentStage(stage_name="GIT_PUSH", status="COMPLETED", details=f"Branch {context.branch_name} was pushed.", evidence={"commit_sha": commit_sha}))
                pr_url = None
                if context.create_pull_request:
                    pr_url = self._create_pull_request(context, commit_sha)
                    stages.append(DeploymentStage(stage_name="PULL_REQUEST", status="COMPLETED", details="Pull request was created or already exists.", evidence={"pull_request_url": pr_url}))
                if askpass:
                    askpass.unlink(missing_ok=True)
                return GitDeploymentResult(status="DEPLOYED", mapping_name=context.mapping_name, branch_name=context.branch_name, commit_sha=commit_sha, remote_url=context.remote_url, pull_request_url=pr_url, deployed_paths=deployed_paths, stages=stages)
        except Exception as exc:
            stages.append(DeploymentStage(stage_name="GIT_DEPLOYMENT", status="FAILED", details=str(exc)[:4000]))
            return GitDeploymentResult(status="FAILED", mapping_name=context.mapping_name, branch_name=context.branch_name, remote_url=context.remote_url, stages=stages, failure_reason=str(exc)[:4000])

    def _validate_context(self, context: GitDeploymentContext) -> None:
        evidence = context.validation_evidence
        if evidence.get("status") != "PASSED" or not evidence.get("deployable"):
            raise RuntimeError("Validation did not mark this mapping deployable.")
        if evidence.get("oracle_strength") != "STRONG":
            raise RuntimeError("Git deployment requires STRONG oracle validation.")
        if not evidence.get("full_reference_coverage"):
            raise RuntimeError("Git deployment requires full reference coverage.")
        if evidence.get("human_review_required"):
            raise RuntimeError("Git deployment is blocked because human review is required.")
        if evidence.get("unsupported_constructs"):
            raise RuntimeError("Git deployment is blocked by unsupported constructs.")
        if not re.fullmatch(r"[A-Za-z0-9._/-]{1,240}", context.branch_name) or ".." in context.branch_name:
            raise ValueError("Invalid Git branch name.")
        if not re.fullmatch(r"[A-Za-z0-9._/-]{1,500}", context.repository_path) or ".." in context.repository_path:
            raise ValueError("Invalid Git repository path.")
        parsed=urlparse(context.remote_url)
        if parsed.scheme not in {"https", "http", "file"} and not Path(context.remote_url).exists():
            raise ValueError("Only HTTP(S), file URLs, and existing local Git repositories are supported.")

    def _verify_artifacts(self, artifacts: list[DeploymentArtifact]) -> list[tuple[DeploymentArtifact, Path]]:
        latest: dict[str, DeploymentArtifact] = {}
        for artifact in artifacts:
            latest.setdefault(artifact.artifact_type, artifact)
        missing=self.REQUIRED_ARTIFACTS-set(latest)
        if missing: raise RuntimeError("Required deployable artifacts are missing: "+", ".join(sorted(missing)))
        result=[]
        for kind in sorted(self.REQUIRED_ARTIFACTS):
            artifact=latest[kind]; path=Path(artifact.storage_path).resolve()
            if not path.is_file(): raise FileNotFoundError(f"Artifact file is missing: {path}")
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            if digest.lower()!=artifact.content_hash.lower(): raise RuntimeError(f"Artifact hash mismatch for {artifact.file_name}.")
            result.append((artifact,path))
        return result

    def _copy_artifacts(self, context, verified, worktree):
        root=(worktree/context.repository_path/self._safe(context.mapping_name)).resolve()
        if worktree.resolve() not in root.parents: raise ValueError("Invalid deployment destination.")
        paths=[]
        for artifact, source in verified:
            folder=self.TYPE_DIRECTORIES[artifact.artifact_type]
            target=(root/folder/Path(artifact.file_name).name).resolve()
            if root not in target.parents: raise ValueError("Invalid artifact destination.")
            target.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(source,target)
            paths.append(target.relative_to(worktree).as_posix())
        return paths

    def _write_manifest(self, context, verified, worktree, deployed_paths):
        root=worktree/context.repository_path/self._safe(context.mapping_name)
        manifest=root/"metadata"/"deployment_manifest.json"; manifest.parent.mkdir(parents=True,exist_ok=True)
        payload={"workflow_id":context.workflow_id,"validation_workflow_id":context.validation_workflow_id,"repository_id":context.repository_id,"mapping_id":context.mapping_id,"mapping_name":context.mapping_name,"deployed_at":datetime.now(timezone.utc).isoformat(),"branch_name":context.branch_name,"validation_evidence":context.validation_evidence,"artifacts":[{"artifact_id":a.artifact_id,"artifact_type":a.artifact_type,"file_name":a.file_name,"content_hash":a.content_hash,"git_path":p} for (a,_),p in zip(verified,deployed_paths,strict=True)]}
        manifest.write_text(json.dumps(payload,indent=2,sort_keys=True),encoding="utf-8"); deployed_paths.append(manifest.relative_to(worktree).as_posix())

    def _clone(self, remote, base, worktree, env):
        subprocess.run(["git","clone","--branch",base,"--single-branch",remote,str(worktree)],env=env,capture_output=True,text=True,check=True,timeout=180)

    @staticmethod
    def _run_git(args, cwd, env):
        try: return subprocess.run(["git",*args],cwd=cwd,env=env,capture_output=True,text=True,check=True,timeout=180)
        except subprocess.CalledProcessError as exc: raise RuntimeError((exc.stderr or exc.stdout or str(exc))[-4000:]) from exc

    @staticmethod
    def _git_environment(token: str | None, temp: Path):
        """Build a non-interactive Git environment without embedding secrets."""
        env = os.environ.copy()
        env.update(
            {
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_CONFIG_NOSYSTEM": "1",
            }
        )
        if not token:
            return env, None

        # Git executes GIT_ASKPASS directly. An explicit POSIX shell shebang
        # prevents the helper from being interpreted with the wrong runtime.
        # The PAT is supplied only through the child-process environment.
        script = temp / "git-askpass.sh"
        script.write_text(
            "#!/bin/sh\n"
            "case \"${1:-}\" in\n"
            "  *[Uu]sername*) printf '%s\\n' \"$NEUFLOW_GIT_USERNAME\" ;;\n"
            "  *) printf '%s\\n' \"$NEUFLOW_GIT_TOKEN\" ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        script.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        env.update(
            {
                "GIT_ASKPASS": str(script),
                "GIT_ASKPASS_REQUIRE": "force",
                "NEUFLOW_GIT_USERNAME": "x-access-token",
                "NEUFLOW_GIT_TOKEN": token,
            }
        )
        return env, script

    def _create_pull_request(self, context, commit_sha):
        parsed=urlparse(context.remote_url)
        if parsed.hostname not in {"github.com","www.github.com"}: raise RuntimeError("Automatic pull-request creation currently supports GitHub HTTPS repositories only.")
        if not context.access_token: raise RuntimeError("A Git access token is required to create a pull request.")
        path=parsed.path.removesuffix(".git").strip("/"); owner,repo=path.split("/",1)
        api=f"https://api.github.com/repos/{owner}/{repo}/pulls"
        headers={"Authorization":f"Bearer {context.access_token}","Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"}
        payload={"title":context.pull_request_title or context.commit_message,"head":context.branch_name,"base":context.base_branch,"body":context.pull_request_body or f"NeuFlow deployment {context.workflow_id}."}
        with httpx.Client(timeout=30.0) as client:
            response=client.post(api,headers=headers,json=payload)
            if response.status_code==422:
                existing=client.get(api,headers=headers,params={"state":"open","head":f"{owner}:{context.branch_name}","base":context.base_branch})
                existing.raise_for_status(); rows=existing.json()
                if rows: return rows[0]["html_url"]
            response.raise_for_status(); return response.json()["html_url"]

    @staticmethod
    def _safe(value): return re.sub(r"[^A-Za-z0-9_.-]+","_",value).strip("._") or "mapping"
