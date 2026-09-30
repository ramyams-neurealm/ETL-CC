from pathlib import Path

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.agents.validation_agent import (
    ReferenceAssumption,
    ReferenceInvariant,
    ReferenceOperation,
    ReferencePlan,
    ReferencePlanValidator,
)


FIXTURE = Path(__file__).parents[1] / "runtime_sources" / "ab_initio_graph" / "sample_ab_initio_graph.json"


def _build_sample_mapping_and_plan():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    plan = ReferencePlan(
        mapping_name=mapping.mapping_name,
        primary_input="SRC_ACCOUNT",
        output_name="TGT_ACTIVE_ACCOUNT",
        comparison_keys=["ACCOUNT_ID"],
        operations=[
            ReferenceOperation(
                operation_id="REF-001",
                operation="FILTER",
                input_name="SRC_ACCOUNT",
                output_name="SRC_ACCOUNT_FILTERED",
                condition='ACCOUNT_STATUS == "ACTIVE"',
            ),
            ReferenceOperation(
                operation_id="REF-002",
                operation="PROJECT",
                input_name="SRC_ACCOUNT_FILTERED",
                output_name="TGT_ACTIVE_ACCOUNT",
                columns=["ACCOUNT_ID", "ACCOUNT_BALANCE", "LOAD_DATE"],
            ),
        ],
        invariants=[
            ReferenceInvariant(
                invariant_id="INV-001",
                invariant_type="OUTPUT_SCHEMA",
                description="Output schema contains the required target fields.",
                fields=["ACCOUNT_ID", "ACCOUNT_BALANCE", "LOAD_DATE"],
            )
        ],
        assumptions=[
            ReferenceAssumption(
                description="The LOAD_DATE parameter is assumed to be set to the current date unless specified otherwise.",
                assumption_type="RUNTIME_DEPENDENCY",
                evidence_gap="runtime date parameter",
                transformation_name="REFORMAT_ACCOUNT",
                source_expression="$LOAD_DATE",
                affected_fields=["LOAD_DATE"],
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
    assert not any("LOAD_DATE" in issue.message for issue in validation.issues)
