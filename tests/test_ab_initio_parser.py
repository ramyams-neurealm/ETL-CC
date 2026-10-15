import json
from pathlib import Path

import pytest

from etl_cc.ab_initio_parser import AbInitioGraphParser, AbInitioGraphParserError
from etl_cc.agents.matchflow_comparator import MatchFlowComparator
from etl_cc.agents.unit_test_specification import UnitTestSpecificationValidator
from etl_cc.agents.validation_agent import ValidationAgent


FIXTURE = Path(__file__).parents[1] / "runtime_sources" / "ab_initio_graph" / "sample_ab_initio_graph.json"


def test_ab_initio_fixture_maps_to_canonical_contract():
    parser = AbInitioGraphParser()
    content = FIXTURE.read_bytes()

    summaries = parser.list_mappings_bytes(content, FIXTURE.name)
    mappings = parser.parse_selected_bytes(content, None, "AB_INITIO", FIXTURE.name)

    assert summaries[0].source_object_key == "accounts/g_load_active_accounts"
    assert mappings[0].source_vendor == "AB_INITIO"
    assert mappings[0].sources[0].name == "SRC_ACCOUNT"
    assert mappings[0].targets[0].name == "TGT_ACTIVE_ACCOUNT"
    assert {item.transformation_type for item in mappings[0].transformations} == {"FILTER", "REFORMAT"}
    assert mappings[0].parameters[0].name == "LOAD_DATE"
    assert mappings[0].parameters[0].default_value == "CURRENT_DATE"
    assert len(mappings[0].connectors) == 11


def test_ab_initio_parser_rejects_other_json_formats():
    content = json.dumps({"format": "INFORMATICA"}).encode()

    with pytest.raises(AbInitioGraphParserError):
        AbInitioGraphParser().list_mappings_bytes(content, "invalid.json")


def test_runtime_parameter_default_is_recognized_as_clock_output():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    runtime_columns = ValidationAgent._runtime_generated_columns(mapping)

    assert "LOAD_DATE" in UnitTestSpecificationValidator.runtime_fields(mapping)
    assert "LOAD_DATE" in runtime_columns

    match = MatchFlowComparator().compare_rows(
        legacy_rows=[
            {"ACCOUNT_ID": 1, "ACCOUNT_BALANCE": "100.00", "LOAD_DATE": "2000-01-01"}
        ],
        pyspark_rows=[
            {"ACCOUNT_ID": 1, "ACCOUNT_BALANCE": "100.00", "LOAD_DATE": "2026-10-08"}
        ],
        keys=["ACCOUNT_ID"],
        ignored_columns=runtime_columns,
    )

    assert match.match_percentage == 100.0
    assert match.sample_differences == []
