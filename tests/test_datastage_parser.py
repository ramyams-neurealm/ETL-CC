import asyncio
import json
from pathlib import Path

import pytest

from etl_cc.agents.lineage_agent import LineageAgent
from etl_cc.datastage_parser import DataStageParser, DataStageParserError


FIXTURES = Path(__file__).parent / "fixtures"

THREE_RECORD_JOB = '''\
BEGIN DSJOB
Identifier "JOB_EMPLOYEE_LOAD"
BEGIN DSRECORD
Identifier "SRC_EMPLOYEE"
OLEType "CTransformerStage"
END DSRECORD
BEGIN DSRECORD
Identifier "TR_EMPLOYEE"
OLEType "Transformer"
END DSRECORD
BEGIN DSRECORD
Identifier "TGT_EMPLOYEE"
OLEType "OracleConnector"
END DSRECORD
END DSJOB
'''


def test_parse_dsx_normalizes_job_to_canonical_mapping():
    parser = DataStageParser()
    mapping = parser.parse(
        FIXTURES / "sample_customers.dsx",
        source_reference="sample_customers.dsx",
    )[0]
    expected = json.loads(
        (FIXTURES / "sample_customers_expected.json").read_text()
    )

    assert mapping.source_vendor == expected["source_vendor"]
    assert mapping.source_object_key == expected["source_object_key"]
    assert mapping.mapping_name == expected["mapping_name"]
    assert [item.name for item in mapping.sources] == expected["sources"]
    assert [item.name for item in mapping.targets] == expected["targets"]
    assert [item.name for item in mapping.transformations] == expected["transformations"]
    assert len(mapping.connectors) == expected["connectors"]
    assert [item.name for item in mapping.parameters] == expected["parameters"]
    assert [item["name"] for item in mapping.workflows] == expected["workflows"]
    assert mapping.source_metadata["stage_count"] == expected["stage_count"]
    assert mapping.source_metadata["link_count"] == expected["link_count"]
    assert mapping.transformations[0].ports[0].expression == "CUSTOMER_ID"

    lineage = asyncio.run(LineageAgent().run(mapping))
    assert [node.node_type for node in lineage.component_nodes] == [
        "SOURCE",
        "TRANSFORMATION",
        "TARGET",
    ]
    assert not {
        "MISSING_SOURCE_DATASETS",
        "MISSING_TARGET_DATASETS",
        "MISSING_COMPONENT_LINKS",
    }.intersection(warning.warning_type for warning in lineage.warnings)


def test_list_mappings_returns_selectable_job_summary():
    summaries = DataStageParser().list_mappings(
        FIXTURES / "sample_customers.dsx", "sample_customers.dsx"
    )

    assert len(summaries) == 1
    assert summaries[0].mapping_name == "Job_Customers"
    assert summaries[0].object_type == "JOB"
    assert summaries[0].source_object_key == "DemoProject/DSJOB/Job_Customers"


def test_selected_keys_filter_jobs():
    parser = DataStageParser()
    mapping = parser.parse(
        FIXTURES / "sample_customers.dsx",
        selected_keys={"other/DSJOB/NotSelected"},
    )

    assert mapping == []


def test_dsrecord_components_are_preserved_and_incomplete_lineage_is_flagged():
    mapping = DataStageParser().parse(THREE_RECORD_JOB)[0]

    assert mapping.source_vendor == "DATASTAGE_DSX"
    assert mapping.source_metadata["stage_count"] == 3
    assert mapping.source_metadata["stage_inventory"] == [
        {
            "name": "SRC_EMPLOYEE",
            "stage_type": "CTransformerStage",
            "role": "TRANSFORM",
        },
        {
            "name": "TR_EMPLOYEE",
            "stage_type": "Transformer",
            "role": "TRANSFORM",
        },
        {
            "name": "TGT_EMPLOYEE",
            "stage_type": "OracleConnector",
            "role": "CONNECTOR",
        },
    ]
    assert [item.name for item in mapping.transformations] == [
        "SRC_EMPLOYEE",
        "TR_EMPLOYEE",
    ]
    assert mapping.sources == []
    assert mapping.targets == []
    assert mapping.connectors == []

    lineage = asyncio.run(LineageAgent().run(mapping))

    assert [node.name for node in lineage.component_nodes] == [
        "SRC_EMPLOYEE",
        "TR_EMPLOYEE",
        "TGT_EMPLOYEE",
    ]
    assert lineage.confidence < 1.0
    assert lineage.human_review_required is True
    assert {warning.warning_type for warning in lineage.warnings} == {
        "MISSING_SOURCE_DATASETS",
        "MISSING_TARGET_DATASETS",
        "MISSING_COMPONENT_LINKS",
    }


def test_rejects_malformed_dsx():
    with pytest.raises(DataStageParserError, match="Unclosed DSX block"):
        DataStageParser().parse("BEGIN DSJOB\n Name \"Broken\"\n")
