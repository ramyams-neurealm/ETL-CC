import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from etl_cc.ab_initio_parser import AbInitioGraphParser, AbInitioGraphParserError
from etl_cc.agents.matchflow_comparator import MatchFlowComparator
from etl_cc.agents.unit_test_specification import (
    InputDatasetSpecification,
    ParameterValue,
    TestCell as SpecificationCell,
    TestRow as SpecificationRow,
    UnitTestScenario,
    UnitTestSpecification,
    UnitTestSpecificationValidator,
)
from etl_cc.agents.validation_agent import (
    SyntheticCell,
    SyntheticDataValidator,
    SyntheticInputRow,
    SyntheticScenario,
    SyntheticTestDataResult,
    ValidationAgent,
)
from etl_cc.worker import _load_mappings


FIXTURE = Path(__file__).parents[1] / "runtime_sources" / "ab_initio_graph" / "sample_ab_initio_graph.json"


def test_ab_initio_fixture_maps_to_canonical_contract():
    parser = AbInitioGraphParser()
    content = FIXTURE.read_bytes()

    summaries = parser.list_mappings_bytes(content, FIXTURE.name)
    mappings = parser.parse_selected_bytes(content, None, "AB_INITIO", FIXTURE.name)

    assert summaries[0].source_object_key == "transactions/g_load_high_value_txns"
    assert mappings[0].source_vendor == "AB_INITIO"
    assert mappings[0].sources[0].name == "SRC_TRANSACTIONS"
    assert mappings[0].targets[0].name == "TGT_HIGH_VALUE_ALERTS"
    assert {item.transformation_type for item in mappings[0].transformations} == {"FILTER", "REFORMAT"}
    assert mappings[0].parameters[0].name == "PROC_DATE"
    assert mappings[0].parameters[0].default_value == "CURRENT_DATE"
    assert len(mappings[0].connectors) == 13


def test_ab_initio_parser_rejects_other_json_formats():
    content = json.dumps({"format": "INFORMATICA"}).encode()

    with pytest.raises(AbInitioGraphParserError):
        AbInitioGraphParser().list_mappings_bytes(content, "invalid.json")


def test_clock_default_parameter_is_a_deterministic_test_input():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    runtime_columns = ValidationAgent._runtime_generated_columns(mapping)

    assert "PROCESSED_ON" not in UnitTestSpecificationValidator.runtime_fields(mapping)
    assert "PROCESSED_ON" in runtime_columns

    match = MatchFlowComparator().compare_rows(
        legacy_rows=[
            {"TXN_ID": "txn-1", "ALERT_AMOUNT": "100.00", "PROCESSED_ON": "2000-01-01"}
        ],
        pyspark_rows=[
            {"TXN_ID": "txn-1", "ALERT_AMOUNT": "100.00", "PROCESSED_ON": "2026-10-08"}
        ],
        keys=["TXN_ID"],
        ignored_columns=runtime_columns,
    )

    assert match.match_percentage == 100.0
    assert match.sample_differences == []


def test_synthetic_validation_accepts_source_key_used_by_derived_target_field():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    generated = SyntheticTestDataResult(
        mapping_name=mapping.mapping_name,
        comparison_keys=["TXN_ID"],
        scenarios=[
            SyntheticScenario(
                test_id="SYN-001",
                category="HAPPY_PATH",
                title="High-value transaction",
                description="A source transaction produces an alert.",
                input_rows=[
                    SyntheticInputRow(
                        cells=[SyntheticCell(field_name="TXN_ID", value="txn-1")]
                    )
                ],
                expected_behavior="The transaction ID is included in ALERT_ID.",
            ),
            SyntheticScenario(
                test_id="SYN-002",
                category="NEGATIVE",
                title="Below-threshold transaction",
                description="A low-value transaction is excluded.",
                input_rows=[
                    SyntheticInputRow(
                        cells=[SyntheticCell(field_name="TXN_ID", value="txn-2")]
                    )
                ],
                expected_behavior="No target alert row is emitted.",
            ),
            SyntheticScenario(
                test_id="SYN-003",
                category="NULL_HANDLING",
                title="Null optional field",
                description="The nullable source field is handled safely.",
                input_rows=[
                    SyntheticInputRow(
                        cells=[SyntheticCell(field_name="TXN_ID", value="txn-3")]
                    )
                ],
                expected_behavior="Null values are handled according to the mapping.",
            ),
        ],
    )

    result = SyntheticDataValidator().run(
        mapping,
        generated,
        {},
        physical_rows=[
            {
                "TXN_ID": "txn-1",
                "CUSTOMER_ID": 1,
                "TXN_AMOUNT": 10000.0,
                "TXN_TYPE": "SALE",
            },
            {
                "TXN_ID": "txn-2",
                "CUSTOMER_ID": 2,
                "TXN_AMOUNT": 5.0,
                "TXN_TYPE": "SALE",
            },
        ],
    )

    comparison_key_check = next(
        check for check in result.checks if check.check_name == "COMPARISON_KEYS"
    )
    assert comparison_key_check.passed
    assert "canonical lineage" in comparison_key_check.details
    assert result.status == "PASSED"


def test_runtime_parameter_is_recognized_without_field_level_connectors():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["graphs"][0]["field_links"] = []
    mapping = AbInitioGraphParser().parse_selected_bytes(
        json.dumps(payload).encode("utf-8"),
        None,
        "AB_INITIO",
        FIXTURE.name,
    )[0]

    assert "PROCESSED_ON" not in UnitTestSpecificationValidator.runtime_fields(mapping)
    specification = UnitTestSpecification(
        mapping_name=mapping.mapping_name,
        scenarios=[
            UnitTestScenario(
                test_id="UTS001",
                title="High-value transaction",
                category="HAPPY_PATH",
                input_datasets=[
                    InputDatasetSpecification(
                        dataset_name="SRC_TRANSACTIONS",
                        rows=[
                            SpecificationRow(
                                cells=[
                                    SpecificationCell(field_name="TXN_ID", value="txn-1"),
                                    SpecificationCell(field_name="CUSTOMER_ID", value=1),
                                    SpecificationCell(field_name="TXN_AMOUNT", value=10000.0),
                                ]
                            )
                        ],
                    )
                ],
                expected_rows=[
                    SpecificationRow(
                        cells=[
                            SpecificationCell(field_name="ALERT_ID", value="ALRT_txn-1"),
                            SpecificationCell(field_name="CUSTOMER_ID", value=1),
                            SpecificationCell(field_name="ALERT_AMOUNT", value=10000.0),
                            SpecificationCell(
                                field_name="PROCESSED_ON",
                                value="2026-10-25",
                            ),
                        ]
                    )
                ],
                parameter_values=[
                    ParameterValue(
                        parameter_name="PROC_DATE",
                        value="2026-10-25",
                    )
                ],
                comparison_keys=["ALERT_ID"],
                deterministic_columns=[
                    "ALERT_ID",
                    "CUSTOMER_ID",
                    "ALERT_AMOUNT",
                    "PROCESSED_ON",
                ],
            )
        ],
    )

    assert UnitTestSpecificationValidator.validate(specification, mapping) == []


@pytest.mark.parametrize(
    "expression",
    [
        "current_timestamp()",
        "to_date(F.expr('current_timestamp()'))",
        "date_format(cast(current_timestamp() as timestamp), 'yyyy-MM-dd')",
    ],
)
def test_direct_and_wrapped_clock_expressions_are_runtime_outputs(expression):
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    mapping.transformations[-1].ports[-1].expression = expression

    assert "PROCESSED_ON" in UnitTestSpecificationValidator.runtime_fields(mapping)


def test_literal_date_expression_is_not_a_runtime_output():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    mapping.transformations[-1].ports[-1].expression = "'2026-10-25'"

    assert "PROCESSED_ON" not in UnitTestSpecificationValidator.runtime_fields(mapping)


def test_discovery_uses_ab_initio_parser_from_workflow_metadata(monkeypatch):
    content = FIXTURE.read_bytes()

    async def load_manifest(_session, _source_id):
        return {
            "config": {"original_file_name": FIXTURE.name},
            "product_code": None,
        }

    async def load_content(_session, _source_id):
        return content

    monkeypatch.setattr("etl_cc.worker.load_manifest", load_manifest)
    monkeypatch.setattr("etl_cc.worker.load_content", load_content)
    repository = SimpleNamespace(
        id=143,
        source_type="UNKNOWN",
        connection_type="UNKNOWN",
    )
    workflow = SimpleNamespace(
        workflow_id="DISC-test",
        scope_payload={
            "source_id": "source-1",
            "product_code": " AB_INITIO ",
            "method_code": "AB_INITIO_GRAPH_UPLOAD",
        }
    )

    mappings = asyncio.run(_load_mappings(None, repository, workflow))

    assert mappings[0].source_vendor == "AB_INITIO"
    assert mappings[0].mapping_name == "g_load_high_value_txns"


def test_discovery_uses_ab_initio_upload_method_when_product_metadata_is_missing(
    monkeypatch,
):
    content = FIXTURE.read_bytes()

    async def load_manifest(_session, _source_id):
        return {"config": {"original_file_name": FIXTURE.name}}

    async def load_content(_session, _source_id):
        return content

    monkeypatch.setattr("etl_cc.worker.load_manifest", load_manifest)
    monkeypatch.setattr("etl_cc.worker.load_content", load_content)
    repository = SimpleNamespace(
        id=143,
        source_type="UNKNOWN",
        connection_type="UNKNOWN",
    )
    workflow = SimpleNamespace(
        workflow_id="DISC-test",
        scope_payload={
            "source_id": "source-1",
            "method_code": " AB_INITIO_GRAPH_UPLOAD ",
        },
    )

    mappings = asyncio.run(_load_mappings(None, repository, workflow))

    assert mappings[0].source_vendor == "AB_INITIO"
    assert mappings[0].mapping_name == "g_load_high_value_txns"
