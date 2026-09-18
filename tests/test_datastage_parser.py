import json
from pathlib import Path

import pytest

from etl_cc.datastage_parser import DataStageParser, DataStageParserError


FIXTURES = Path(__file__).parent / "fixtures"


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


def test_rejects_malformed_dsx():
    with pytest.raises(DataStageParserError, match="Unclosed DSX block"):
        DataStageParser().parse("BEGIN DSJOB\n Name \"Broken\"\n")
