"""PostgreSQL-backed storage for validation input datasets."""
from __future__ import annotations
import hashlib
import io
import json
import re
import uuid
from pathlib import Path
from typing import Any
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from etl_cc.config import settings
from etl_cc.models import ValidationDatasetETL

class ValidationStore:
    """Validate, persist, and load datasets without persistent server files."""
    ALLOWED_SUFFIXES = {".csv", ".json", ".parquet"}
    FORMAT_BY_SUFFIX = {".csv": "CSV", ".json": "JSON", ".parquet": "PARQUET"}
    UPLOAD_ID_PATTERN = re.compile(r"UPL-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

    def parse(self, file_name: str, content: bytes) -> list[dict[str, Any]]:
        """Parse supported content entirely in memory and enforce limits."""
        if not content:
            raise ValueError("Validation dataset is empty.")
        if len(content) > settings.validation_upload_max_bytes:
            raise ValueError("Validation dataset exceeds configured size limit.")
        suffix = Path(Path(file_name).name).suffix.lower()
        if suffix not in self.ALLOWED_SUFFIXES:
            raise ValueError("Only CSV, JSON, and Parquet datasets are supported.")
        stream = io.BytesIO(content)
        if suffix == ".csv":
            frame = pd.read_csv(stream)
        elif suffix == ".parquet":
            frame = pd.read_parquet(stream)
        else:
            try:
                frame = pd.read_json(stream)
            except ValueError:
                stream.seek(0)
                frame = pd.read_json(stream, lines=True)
        if len(frame) > settings.validation_upload_max_rows:
            raise ValueError("Validation dataset exceeds configured row limit.")
        frame = frame.where(pd.notnull(frame), None)
        return json.loads(frame.to_json(orient="records", date_format="iso"))

    async def stage(self, session: AsyncSession, file_name: str, content: bytes) -> dict[str, Any]:
        """Validate and persist an uploaded dataset in PostgreSQL only."""
        original_name = Path(file_name).name or "dataset"
        suffix = Path(original_name).suffix.lower()
        rows = self.parse(original_name, content)
        upload_id = f"UPL-{uuid.uuid4()}"
        columns = sorted({str(key) for row in rows for key in row})
        session.add(ValidationDatasetETL(
            upload_id=upload_id, file_name=original_name,
            file_format=self.FORMAT_BY_SUFFIX[suffix], content_binary=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_size_bytes=len(content), row_count=len(rows),
            detected_columns=columns,
        ))
        await session.commit()
        return {"upload_id": upload_id, "file_name": original_name,
                "file_format": self.FORMAT_BY_SUFFIX[suffix],
                "row_count": len(rows), "detected_columns": columns}

    async def load_upload(self, session: AsyncSession, upload_id: str) -> list[dict[str, Any]]:
        """Load and parse one PostgreSQL-backed validation dataset."""
        if not self.UPLOAD_ID_PATTERN.fullmatch(upload_id or ""):
            raise ValueError("Invalid Validation upload ID.")
        row = await session.scalar(select(ValidationDatasetETL).where(ValidationDatasetETL.upload_id == upload_id))
        if row is None:
            raise ValueError("Validation upload was not found.")
        if hashlib.sha256(row.content_binary).hexdigest() != row.content_hash:
            raise ValueError("Validation upload content integrity check failed.")
        return self.parse(row.file_name, row.content_binary)

validation_store = ValidationStore()
