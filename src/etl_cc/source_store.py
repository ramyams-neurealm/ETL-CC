"""PostgreSQL-backed transient source store.

Uploaded source content is retained only between analysis and discovery. After successful
canonical mapping persistence, source_content is cleared in the same database
transaction. No runtime_sources directory is used.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.config import settings
from etl_cc.models import SourceSnapshotETL, SourceSnapshotFileETL
from etl_cc.xml_package import merge_xml_files


async def create_source(
    session: AsyncSession,
    connection_type: str,
    manifest: dict,
    content: bytes | None = None,
) -> tuple[str, str | None]:
    if content is not None and len(content) > settings.max_source_upload_bytes:
        raise ValueError("The uploaded source exceeds the configured size limit.")
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


async def load_package_files(
    session: AsyncSession,
    source_id: str,
) -> list[tuple[str, bytes, dict]]:
    """Load and integrity-check every XML member in a source package."""
    parent = await get_source_row(session, source_id)
    rows = list(
        (
            await session.scalars(
                select(SourceSnapshotFileETL)
                .where(SourceSnapshotFileETL.source_snapshot_id == parent.id)
                .order_by(SourceSnapshotFileETL.file_name)
            )
        ).all()
    )
    package_files: list[tuple[str, bytes, dict]] = []
    for package_file in rows:
        content = bytes(package_file.content_binary)
        actual_hash = hashlib.sha256(content).hexdigest()
        if actual_hash != package_file.content_hash:
            raise ValueError(
                "Source package file integrity validation failed: "
                f"{package_file.file_name}"
            )
        package_files.append(
            (
                package_file.file_name,
                content,
                dict(package_file.object_summary or {}),
            )
        )
    return package_files


async def load_content(session: AsyncSession, source_id: str) -> bytes:
    """Load legacy single XML or merge a multi-file XML package in memory."""
    row = await get_source_row(session, source_id)
    if row.source_content is not None:
        content = bytes(row.source_content)
        if hashlib.sha256(content).hexdigest() != row.content_hash:
            raise ValueError("Source content integrity validation failed.")
        return content

    package_files = await load_package_files(session, source_id)
    if not package_files:
        raise FileNotFoundError("The temporary XML payload is unavailable.")

    return merge_xml_files(
        [(file_name, content) for file_name, content, _ in package_files]
    )


async def consume_source_content(session: AsyncSession, source_id: str) -> None:
    """Remove transient single-file or package content after canonicalization."""
    row = await get_source_row(session, source_id)
    row.source_content = None
    await session.execute(
        delete(SourceSnapshotFileETL).where(
            SourceSnapshotFileETL.source_snapshot_id == row.id
        )
    )
    row.status = "CANONICALIZED"
    await session.flush()


async def delete_source(session: AsyncSession, source_id: str) -> None:
    await session.execute(
        delete(SourceSnapshotETL).where(SourceSnapshotETL.source_id == source_id)
    )


async def create_source_package(session, connection_type, manifest, files):
    package_hash=hashlib.sha256()
    for name,content,summary in sorted(files,key=lambda x:x[0].casefold()):
        package_hash.update(name.encode()); package_hash.update(b"\0"); package_hash.update(content)
    source_id,_=await create_source(session,connection_type,manifest,None)
    parent=await get_source_row(session,source_id)
    parent.content_hash=package_hash.hexdigest(); parent.content_size_bytes=sum(len(x[1]) for x in files)
    for name,content,summary in files:
        session.add(SourceSnapshotFileETL(source_snapshot_id=parent.id,file_name=name,content_binary=content,content_hash=hashlib.sha256(content).hexdigest(),content_size_bytes=len(content),object_summary=summary))
    await session.flush(); return source_id,parent.content_hash
