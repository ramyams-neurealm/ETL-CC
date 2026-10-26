import asyncio
import json
from pathlib import Path

import pytest

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.agents.conversion_agent import (
    ConversionAgent,
    ConversionResult,
    GeneratedFile,
)
from etl_cc.agents.unit_test_specification import (
    InputDatasetSpecification,
    ParameterValue,
    RuntimeColumnExpectation,
    TestCell as SpecCell,
    TestRow as SpecRow,
    UnitTestScenario,
    UnitTestSpecification,
    UnitTestSpecificationValidator,
)
from etl_cc.agents.unit_test_renderer import UnitTestRenderer


FIXTURE = (
    Path(__file__).parents[1]
    / "runtime_sources"
    / "ab_initio_graph"
    / "sample_ab_initio_graph.json"
)


def _specification(
    *,
    with_fixed_parameter: bool,
    declares_runtime: bool,
    include_expected_date: bool = False,
):
    expected_cells = [
        SpecCell(field_name="ALERT_ID", value="ALRT_txn-1"),
        SpecCell(field_name="CUSTOMER_ID", value=1),
        SpecCell(field_name="ALERT_AMOUNT", value=10000.0),
    ]
    deterministic_columns = ["ALERT_ID", "CUSTOMER_ID", "ALERT_AMOUNT"]
    runtime_columns = []
    if with_fixed_parameter or include_expected_date:
        expected_cells.append(
            SpecCell(field_name="PROCESSED_ON", value="2026-10-25")
        )
        deterministic_columns.append("PROCESSED_ON")
    if declares_runtime:
        runtime_columns.append(
            RuntimeColumnExpectation(
                column_name="PROCESSED_ON",
                expected_type="date",
            )
        )

    return UnitTestSpecification(
        mapping_name="g_load_high_value_txns",
        scenarios=[
            UnitTestScenario(
                test_id="UTS001",
                title="High-value transaction uses supplied processing date",
                category="HAPPY_PATH",
                input_datasets=[
                    InputDatasetSpecification(
                        dataset_name="SRC_TRANSACTIONS",
                        rows=[
                            SpecRow(
                                cells=[
                                    SpecCell(field_name="TXN_ID", value="txn-1"),
                                    SpecCell(field_name="CUSTOMER_ID", value=1),
                                    SpecCell(field_name="TXN_AMOUNT", value=10000.0),
                                ]
                            )
                        ],
                    )
                ],
                expected_rows=[SpecRow(cells=expected_cells)],
                comparison_keys=["ALERT_ID"],
                deterministic_columns=deterministic_columns,
                parameter_values=(
                    [
                        ParameterValue(
                            parameter_name="PROC_DATE",
                            value="2026-10-25",
                        )
                    ]
                    if with_fixed_parameter
                    else []
                ),
                runtime_columns=runtime_columns,
            )
        ],
    )


def test_retry_prompt_includes_exact_errors_and_previous_scenarios(monkeypatch):
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    invalid = _specification(with_fixed_parameter=False, declares_runtime=True)
    valid = _specification(with_fixed_parameter=True, declares_runtime=False)
    expected_errors = UnitTestSpecificationValidator.validate(invalid, mapping)
    calls = []

    class _FakeLLM:
        def __init__(self, **_kwargs):
            self.response_index = 0

        def with_structured_output(self, *_args, **_kwargs):
            return self

        async def ainvoke(self, messages):
            calls.append(messages)
            parsed = [invalid, valid][len(calls) - 1]
            return {"parsed": parsed, "raw": None}

    monkeypatch.setattr(
        "etl_cc.agents.conversion_agent.DynamicChatOpenAI",
        _FakeLLM,
    )
    result = ConversionResult(
        mapping_name=mapping.mapping_name,
        target_platform="DATABRICKS",
        target_framework="PYSPARK",
        generated_files=[
            GeneratedFile(
                artifact_type="PYSPARK_CODE",
                file_name="transform.py",
                content=(
                    "from pyspark.sql import DataFrame\n"
                    "from typing import Any\n"
                    "def transform(inputs: dict[str, DataFrame], *, "
                    "parameters: dict[str, Any] | None = None) -> DataFrame:\n"
                    "    return inputs['SRC_TRANSACTIONS']\n"
                ),
                media_type="text/x-python",
            ),
            GeneratedFile(
                artifact_type="UNIT_TEST",
                file_name="test_transform.py",
                content="",
                media_type="text/x-python",
            ),
        ],
        confidence=0.9,
    )

    asyncio.run(
        ConversionAgent()._replace_unit_test_with_rendered_specification(
            result,
            mapping,
            {},
        )
    )

    retry_payload = json.loads(calls[1][1][1])
    assert retry_payload["previous_validation_errors"] == expected_errors
    assert retry_payload["previous_specification"]["scenarios"][0]["test_id"] == "UTS001"
    assert "parameters={'PROC_DATE': date.fromisoformat" in result.generated_files[1].content


def test_literal_date_is_expected_exactly_in_rendered_test():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    mapping.transformations[-1].ports[-1].expression = "'2026-10-25'"
    specification = _specification(
        with_fixed_parameter=False,
        declares_runtime=False,
        include_expected_date=True,
    )

    assert UnitTestSpecificationValidator.validate(specification, mapping) == []
    rendered = UnitTestRenderer.render(specification, mapping, "transform.py")

    assert "actual = transform(inputs)" in rendered
    assert "date.fromisoformat('2026-10-25')" in rendered
    assert "'PROCESSED_ON'" in rendered


@pytest.mark.parametrize(
    "expression",
    [
        "current_timestamp()",
        "to_date(F.expr('current_timestamp()'))",
    ],
)
def test_direct_and_wrapped_clock_expressions_render_runtime_assertions(
    expression,
):
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    mapping.transformations[-1].ports[-1].expression = expression
    specification = _specification(
        with_fixed_parameter=False,
        declares_runtime=True,
    )

    assert UnitTestSpecificationValidator.validate(specification, mapping) == []
    rendered = UnitTestRenderer.render(specification, mapping, "transform.py")

    assert "actual = transform(inputs)" in rendered
    assert "assert 'PROCESSED_ON' in actual.columns" in rendered
    assert "DateType" in rendered
