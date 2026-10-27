from pathlib import Path
from datetime import date

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.agents.validation_agent import (
    ReferenceAssumption,
    ReferenceExpression,
    ReferenceInvariant,
    ReferenceOperation,
    ReferencePlan,
    ReferencePlanValidator,
    RelationalIRExecutor,
    SafeExpression,
)


FIXTURE = Path(__file__).parents[1] / "runtime_sources" / "ab_initio_graph" / "sample_ab_initio_graph.mp"


def _build_sample_mapping_and_plan():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    target_fields = ["ALERT_ID", "CUSTOMER_ID", "ALERT_AMOUNT", "PROCESSED_ON"]
    plan = ReferencePlan(
        mapping_name=mapping.mapping_name,
        primary_input="SRC_TRANSACTIONS",
        output_name="TGT_HIGH_VALUE_ALERTS",
        comparison_keys=["ALERT_ID"],
        operations=[
            ReferenceOperation(
                operation_id="REF-001",
                operation="FILTER",
                input_name="SRC_TRANSACTIONS",
                output_name="FILTER_HIGH_VALUE",
                condition="TXN_AMOUNT >= 10000.00",
            ),
            ReferenceOperation(
                operation_id="REF-002",
                operation="DERIVE",
                input_name="FILTER_HIGH_VALUE",
                output_name="REFORMAT_TO_ALERT",
                expressions=[
                    ReferenceExpression(output_field="ALERT_ID", expression='CONCAT("ALRT_", TXN_ID)'),
                    ReferenceExpression(output_field="CUSTOMER_ID", expression="CUSTOMER_ID"),
                    ReferenceExpression(output_field="ALERT_AMOUNT", expression="TXN_AMOUNT"),
                    ReferenceExpression(output_field="PROCESSED_ON", expression="CURRENT_DATE"),
                ],
            ),
            ReferenceOperation(
                operation_id="REF-003",
                operation="PROJECT",
                input_name="REFORMAT_TO_ALERT",
                output_name="TGT_HIGH_VALUE_ALERTS",
                columns=target_fields,
            ),
        ],
        invariants=[
            ReferenceInvariant(
                invariant_id="INV-001",
                invariant_type="OUTPUT_SCHEMA",
                description="Output schema contains the required target fields.",
                fields=target_fields,
            )
        ],
        assumptions=[
            ReferenceAssumption(
                description="The PROC_DATE parameter is assumed to be set to the current date unless specified otherwise.",
                assumption_type="RUNTIME_DEPENDENCY",
                evidence_gap="runtime date parameter",
                transformation_name="REFORMAT_TO_ALERT",
                source_expression="$PROC_DATE",
                affected_fields=["PROCESSED_ON"],
            )
        ],
    )
    return mapping, plan


def test_runtime_parameter_assumption_is_not_blocking_for_reference_plan_validation():
    mapping, plan = _build_sample_mapping_and_plan()
    validation = ReferencePlanValidator().run(mapping, plan)

    assert validation.status == "PASSED"
    assert validation.safe_to_execute is True
    assert validation.full_reference_coverage is True
    assert not any("PROCESSED_ON" in issue.message for issue in validation.issues)


def test_current_date_is_supported_in_reference_expressions():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    plan = ReferencePlan(
        mapping_name=mapping.mapping_name,
        primary_input="SRC_TRANSACTIONS",
        output_name="TGT_HIGH_VALUE_ALERTS",
        comparison_keys=["ALERT_ID"],
        operations=[
            ReferenceOperation(
                operation_id="REF-001",
                operation="DERIVE",
                input_name="SRC_TRANSACTIONS",
                output_name="REFORMAT_TO_ALERT",
                expressions=[
                    ReferenceExpression(
                        output_field="ALERT_ID",
                        expression='CONCAT("ALRT_", TXN_ID)',
                    ),
                    ReferenceExpression(
                        output_field="CUSTOMER_ID",
                        expression="CUSTOMER_ID",
                    ),
                    ReferenceExpression(
                        output_field="ALERT_AMOUNT",
                        expression="TXN_AMOUNT",
                    ),
                    ReferenceExpression(
                        output_field="PROCESSED_ON",
                        expression="CURRENT_DATE",
                    ),
                ],
            ),
            ReferenceOperation(
                operation_id="REF-002",
                operation="PROJECT",
                input_name="REFORMAT_TO_ALERT",
                output_name="TGT_HIGH_VALUE_ALERTS",
                columns=[
                    "ALERT_ID",
                    "CUSTOMER_ID",
                    "ALERT_AMOUNT",
                    "PROCESSED_ON",
                ],
            ),
        ],
    )

    assert SafeExpression.validate("CURRENT_DATE", set()) == []
    assert SafeExpression.evaluate("CURRENT_DATE", {}) == date(2000, 1, 1)

    validation = ReferencePlanValidator().run(mapping, plan)
    assert validation.safe_to_execute
    result = RelationalIRExecutor().run(
        mapping,
        plan,
        validation,
        {
            "SRC_TRANSACTIONS": [
                {
                    "TXN_ID": "txn-1",
                    "CUSTOMER_ID": 1,
                    "TXN_AMOUNT": 12000.0,
                    "TXN_TYPE": "SALE",
                }
            ]
        },
    )

    assert result.status == "PASSED"
    assert result.rows[0]["PROCESSED_ON"] == date(2000, 1, 1)
