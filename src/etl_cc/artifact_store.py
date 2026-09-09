"""Safe local artifact persistence for conversion workflows."""

import hashlib
import re
from pathlib import Path

from etl_cc.config import settings


class ArtifactStoreError(RuntimeError):
    pass


class ArtifactStore:
    def _safe_name(self, value: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
        if not safe:
            raise ArtifactStoreError("Artifact name is empty after sanitization.")
        return safe

    def save_text(
        self,
        *,
        workflow_id: str,
        mapping_name: str,
        file_name: str,
        content: str,
    ) -> tuple[Path, str]:
        if not content.strip():
            raise ArtifactStoreError("Generated artifact content is empty.")
        root = settings.artifact_directory.resolve()
        directory = (
            root
            / self._safe_name(workflow_id)
            / self._safe_name(mapping_name)
        ).resolve()
        if root not in directory.parents:
            raise ArtifactStoreError("Resolved artifact path escaped the artifact root.")
        directory.mkdir(parents=True, exist_ok=True)
        path = (directory / self._safe_name(file_name)).resolve()
        if directory not in path.parents:
            raise ArtifactStoreError("Resolved file path escaped the mapping directory.")
        path.write_text(content, encoding="utf-8", newline="\n")
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return path, digest


artifact_store = ArtifactStore()
