"""PostgreSQL-backed transient source store.

Uploaded source exports are retained only between analysis and discovery.
After successful canonical mapping persistence, source_content is cleared in
the same database transaction. No runtime_sources directory is used.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.config import settings
from etl_cc.models import SourceSnapshotETL


async def create_source(
    session: AsyncSession,
    connection_type: str,
    manifest: dict,
    content: bytes | None = None,
) -> tuple[str, str | None]:
    if content is not None and len(content) > settings.max_xml_upload_bytes:
        raise ValueError("The uploaded XML exceeds the configured size limit.")
    source_id = str(uuid4())
    digest = hashlib.sha256(content).hexdigest() if content is not None else None
    session.add(SourceSnapshotETL(
        source_id=source_id,
        connection_type=connection_type,
        manifest={**manifest, "source_id": source_id},
        source_content=content,
        content_hash=digest,
        content_size_bytes=len(content or b""),
        status="PENDING",
        expires_at=datetime.now(timezone.utc) + timedelta(
            seconds=settings.source_snapshot_ttl_seconds
        ),
    ))
    await session.flush()
    return source_id, digest


async def update_manifest(session: AsyncSession, source_id: str, manifest: dict) -> None:
    row = await get_source_row(session, source_id)
    row.manifest = {**manifest, "source_id": source_id}
    await session.flush()


async def get_source_row(session: AsyncSession, source_id: str) -> SourceSnapshotETL:
    row = await session.scalar(
        select(SourceSnapshotETL).where(SourceSnapshotETL.source_id == source_id)
    )
    if row is None:
        raise FileNotFoundError("Source reference was not found.")
    if row.expires_at and row.expires_at < datetime.now(timezone.utc):
        raise FileNotFoundError("Source reference expired. Analyze the source again.")
    return row


async def load_manifest(session: AsyncSession, source_id: str) -> dict:
    return dict((await get_source_row(session, source_id)).manifest)


async def load_content(session: AsyncSession, source_id: str) -> bytes:
    row = await get_source_row(session, source_id)
    if row.source_content is None:
        raise FileNotFoundError("The temporary XML payload is unavailable.")
    content = bytes(row.source_content)
    if hashlib.sha256(content).hexdigest() != row.content_hash:
        raise ValueError("Source content integrity validation failed.")
    return content


async def consume_source_content(session: AsyncSession, source_id: str) -> None:
    row = await get_source_row(session, source_id)
    row.source_content = None
    row.status = "CANONICALIZED"
    await session.flush()


async def delete_source(session: AsyncSession, source_id: str) -> None:
    await session.execute(
        delete(SourceSnapshotETL).where(SourceSnapshotETL.source_id == source_id)
    )
