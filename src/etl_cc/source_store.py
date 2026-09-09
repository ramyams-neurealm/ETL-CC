"""Temporary durable source store used between analysis and discovery."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

from etl_cc.config import PROJECT_ROOT

SOURCE_ROOT = PROJECT_ROOT / "runtime_sources"


def save_bytes(connection_type: str, file_name: str, content: bytes) -> tuple[str, Path, str]:
    source_id = str(uuid4())
    directory = SOURCE_ROOT / connection_type.lower() / source_id
    directory.mkdir(parents=True, exist_ok=False)
    safe_name = Path(file_name).name
    file_path = directory / safe_name
    file_path.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    return source_id, file_path, digest


def write_manifest(source_id: str, directory: Path, payload: dict) -> Path:
    manifest = directory / "source.json"
    manifest.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return manifest


def load_manifest(source_id: str) -> dict:
    matches = list(SOURCE_ROOT.glob(f"*/{source_id}/source.json"))
    if len(matches) != 1:
        raise FileNotFoundError("Source reference was not found or is ambiguous.")
    return json.loads(matches[0].read_text(encoding="utf-8"))
