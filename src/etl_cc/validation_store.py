"""Safe storage and loading for Validation input datasets."""

from __future__ import annotations

import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from etl_cc.config import settings


class ValidationStore:
    """Stage, resolve, and load Validation input datasets safely."""

    ALLOWED_SUFFIXES = {".csv", ".json", ".parquet"}
    FORMAT_BY_SUFFIX = {
        ".csv": "CSV",
        ".json": "JSON",
        ".parquet": "PARQUET",
    }
    UPLOAD_ID_PATTERN = re.compile(
        r"UPL-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    )

    @staticmethod
    def _safe(value: str) -> str:
        """Return a filesystem-safe name component."""
        safe_value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
        return safe_value or "validation"

    @staticmethod
    def _is_direct_child(path: Path, parent: Path) -> bool:
        """Return True when path is an immediate child of parent."""
        return path.parent == parent

    def _root(self) -> Path:
        """Return the absolute Validation upload root."""
        root = Path(settings.validation_upload_directory).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def stage(self, file_name: str, content: bytes) -> dict[str, Any]:
        """Validate and stage one uploaded CSV, JSON, or Parquet dataset."""
        if not content:
            raise ValueError("Validation dataset is empty.")

        if len(content) > settings.validation_upload_max_bytes:
            raise ValueError("Validation dataset exceeds configured size limit.")

        original_name = Path(file_name).name
        suffix = Path(original_name).suffix.lower()
        if suffix not in self.ALLOWED_SUFFIXES:
            raise ValueError(
                "Only CSV, JSON, and Parquet datasets are supported."
            )

        root = self._root()
        upload_id = f"UPL-{uuid.uuid4()}"
        directory = (root / upload_id).resolve()

        if not self._is_direct_child(directory, root):
            raise ValueError("Invalid Validation storage path.")

        directory.mkdir(parents=True, exist_ok=False)
        data_path = (directory / f"dataset{suffix}").resolve()

        try:
            if not self._is_direct_child(data_path, directory):
                raise ValueError("Invalid Validation dataset path.")

            data_path.write_bytes(content)
            rows = self.load_path(data_path)

            if len(rows) > settings.validation_upload_max_rows:
                raise ValueError(
                    "Validation dataset exceeds configured row limit."
                )

            metadata = {
                "upload_id": upload_id,
                "file_name": original_name,
                "file_format": self.FORMAT_BY_SUFFIX[suffix],
                "row_count": len(rows),
                "detected_columns": sorted(
                    {column for row in rows for column in row}
                ),
                "storage_path": str(data_path),
            }

            metadata_path = directory / "metadata.json"
            metadata_path.write_text(
                json.dumps(metadata, indent=2),
                encoding="utf-8",
            )
        except Exception:
            shutil.rmtree(directory, ignore_errors=True)
            raise

        return {
            key: value
            for key, value in metadata.items()
            if key != "storage_path"
        }

    def resolve_upload(self, upload_id: str) -> Path:
        """Resolve and validate one completed staged Validation dataset."""
        if not self.UPLOAD_ID_PATTERN.fullmatch(upload_id):
            raise ValueError("Invalid upload ID.")

        root = self._root()
        directory = (root / upload_id).resolve()

        if not self._is_direct_child(directory, root) or not directory.is_dir():
            raise ValueError("Validation upload was not found.")

        metadata_path = (directory / "metadata.json").resolve()
        if not self._is_direct_child(metadata_path, directory):
            raise ValueError("Invalid Validation metadata path.")
        if not metadata_path.is_file():
            raise ValueError("Validation upload metadata is missing.")

        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("Validation upload metadata is invalid.") from exc

        if metadata.get("upload_id") != upload_id:
            raise ValueError(
                "Validation upload metadata does not match the requested upload ID."
            )

        # metadata.json is deliberately excluded even though .json is an
        # allowed dataset format.
        dataset_files = [
            path.resolve()
            for path in directory.iterdir()
            if (
                path.is_file()
                and path.name != "metadata.json"
                and path.suffix.lower() in self.ALLOWED_SUFFIXES
            )
        ]

        if len(dataset_files) != 1:
            names = sorted(path.name for path in dataset_files)
            raise ValueError(
                "Validation upload must contain exactly one staged dataset "
                f"file. Found: {names}."
            )

        data_path = dataset_files[0]
        if not self._is_direct_child(data_path, directory):
            raise ValueError("Validation upload contains an invalid dataset path.")
        if not data_path.is_file():
            raise ValueError("Validation uploaded dataset file is missing.")
        if data_path.stat().st_size <= 0:
            raise ValueError("Validation uploaded dataset is empty.")

        expected_format = str(metadata.get("file_format", "")).strip().upper()
        actual_format = self.FORMAT_BY_SUFFIX.get(data_path.suffix.lower())
        if actual_format is None:
            raise ValueError("Validation uploaded dataset format is unsupported.")
        if expected_format and expected_format != actual_format:
            raise ValueError(
                "Validation upload metadata format does not match the staged "
                f"dataset. Metadata: {expected_format}; file: {actual_format}."
            )

        storage_path_value = metadata.get("storage_path")
        if storage_path_value:
            metadata_data_path = Path(storage_path_value).expanduser().resolve()
            if metadata_data_path != data_path:
                raise ValueError(
                    "Validation upload metadata contains an inconsistent "
                    "storage path."
                )

        return data_path

    def load_upload(self, upload_id: str) -> list[dict[str, Any]]:
        """Load one staged Validation dataset by upload ID."""
        return self.load_path(self.resolve_upload(upload_id))

    def load_path(self, path: Path) -> list[dict[str, Any]]:
        """Load CSV, JSON, JSON Lines, or Parquet rows from a local file."""
        path = Path(path).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"Validation dataset was not found: {path}")
        if path.stat().st_size <= 0:
            raise ValueError("Validation dataset is empty.")

        suffix = path.suffix.lower()
        if suffix == ".csv":
            frame = pd.read_csv(path)
        elif suffix == ".parquet":
            frame = pd.read_parquet(path)
        elif suffix == ".json":
            try:
                frame = pd.read_json(path)
            except ValueError:
                frame = pd.read_json(path, lines=True)
        else:
            raise ValueError("Unsupported Validation dataset format.")

        if len(frame) > settings.validation_upload_max_rows:
            raise ValueError("Validation dataset exceeds configured row limit.")

        frame = frame.where(pd.notnull(frame), None)
        return json.loads(frame.to_json(orient="records", date_format="iso"))

    def save(
        self,
        workflow_id: str,
        role: str,
        file_name: str,
        content: bytes,
    ) -> Path:
        """Save one comparison file under a workflow-specific directory."""
        if not content:
            raise ValueError("Validation comparison file is empty.")

        suffix = Path(file_name).suffix.lower()
        if suffix not in self.ALLOWED_SUFFIXES:
            raise ValueError(
                "Only CSV, JSON, and Parquet comparison files are supported."
            )

        root = self._root()
        directory = (root / self._safe(workflow_id)).resolve()
        if not self._is_direct_child(directory, root):
            raise ValueError("Invalid Validation comparison directory.")
        directory.mkdir(parents=True, exist_ok=True)

        path = (directory / f"{self._safe(role)}{suffix}").resolve()
        if not self._is_direct_child(path, directory):
            raise ValueError("Invalid Validation comparison file path.")

        path.write_bytes(content)
        return path


validation_store = ValidationStore()
