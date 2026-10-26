import asyncio
from types import SimpleNamespace

import pytest

from etl_cc.models import StartConversionRequest
from etl_cc.services import start_conversion


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _Session:
    def __init__(self, parent):
        self.parent = parent
        self.added = []
        self.mapping = SimpleNamespace(
            id=135,
            object_name="mapping",
            discovery_status="ANALYZED",
            has_dependency_cycle=False,
            migration_wave=1,
            migration_status="READY",
        )

    async def get(self, _model, repository_id):
        return SimpleNamespace(id=repository_id)

    async def scalars(self, _query):
        return _Rows([self.mapping])

    async def scalar(self, _query):
        return self.parent

    def add(self, row):
        self.added.append(row)

    async def flush(self):
        pass

    async def commit(self):
        pass


def test_start_conversion_links_to_requested_migration(monkeypatch):
    parent = SimpleNamespace(
        workflow_id="MIG-1",
        job_type="MIGRATION",
        repository_id=88,
        scope_payload={"etl_object_ids": [135]},
    )
    session = _Session(parent)
    async def latest_discovery(*args, **kwargs):
        return SimpleNamespace(
            response_payload={"gliner_decision": "AUTO_CONVERT"}
        )

    monkeypatch.setattr(
        "etl_cc.services._latest_agent_response",
        latest_discovery,
    )
    request = StartConversionRequest(
        repository_id=88,
        etl_object_ids=[135],
        migration_id="MIG-1",
    )

    asyncio.run(start_conversion(session, request))

    conversion = next(row for row in session.added if row.job_type == "CONVERSION")
    assert conversion.batch_id == "MIG-1"


def test_start_conversion_rejects_parent_from_another_repository():
    parent = SimpleNamespace(
        workflow_id="MIG-1",
        job_type="MIGRATION",
        repository_id=99,
        scope_payload={"etl_object_ids": [135]},
    )
    session = _Session(parent)
    request = StartConversionRequest(
        repository_id=88,
        etl_object_ids=[135],
        migration_id="MIG-1",
    )

    with pytest.raises(ValueError, match="Migration not found"):
        asyncio.run(start_conversion(session, request))

    assert session.added == []