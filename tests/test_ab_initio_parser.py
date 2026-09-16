import json
from pathlib import Path

import pytest

from etl_cc.ab_initio_parser import AbInitioGraphParser, AbInitioGraphParserError


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
