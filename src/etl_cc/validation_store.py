"""Safe storage for uploaded MatchFlow comparison datasets."""

import re
from pathlib import Path
from etl_cc.config import settings


class ValidationStore:
    def save(self, workflow_id: str, role: str, file_name: str, content: bytes) -> Path:
        if not content:
            raise ValueError(f"{role} comparison file is empty.")
        suffix = Path(file_name).suffix.lower()
        if suffix not in {".csv", ".json"}:
            raise ValueError("Only CSV and JSON comparison files are supported.")
        safe_workflow = re.sub(r"[^A-Za-z0-9_.-]+", "_", workflow_id)
        root = settings.validation_upload_directory.resolve()
        directory = (root / safe_workflow).resolve()
        if root not in directory.parents:
            raise ValueError("Invalid validation storage path.")
        directory.mkdir(parents=True, exist_ok=True)
        path = (directory / f"{role}{suffix}").resolve()
        if directory not in path.parents:
            raise ValueError("Invalid validation file path.")
        path.write_bytes(content)
        return path


validation_store = ValidationStore()
