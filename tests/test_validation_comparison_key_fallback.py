from pathlib import Path

import pytest

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.agents.validation_agent import ValidationAgent


FIXTURE = (
    Path(__file__).parents[1]
    / "runtime_sources"
    / "ab_initio_graph"
    / "sample_ab_initio_graph.mp"
)


def test_duplicate_requested_key_falls_back_to_unique_target_identity():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    legacy_rows = [
        {"CUSTOMER_ID": 12345, "ALERT_ID": "ALRT_txn-1"},
        {"CUSTOMER_ID": 12345, "ALERT_ID": "ALRT_txn-2"},
    ]
    target_rows = [dict(row) for row in legacy_rows]

    keys = ValidationAgent._select_unique_comparison_keys(
        mapping,
        ["CUSTOMER_ID"],
        legacy_rows,
        target_rows,
        {"PROCESSED_ON"},
    )

    assert keys == ["ALERT_ID"]


def test_unique_requested_comparison_key_is_preserved():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    rows = [
        {"ALERT_ID": "ALRT_txn-1", "CUSTOMER_ID": 12345},
        {"ALERT_ID": "ALRT_txn-2", "CUSTOMER_ID": 12345},
    ]

    keys = ValidationAgent._select_unique_comparison_keys(
        mapping,
        ["ALERT_ID"],
        rows,
        rows,
        {"PROCESSED_ON"},
    )

    assert keys == ["ALERT_ID"]


def test_no_unique_grounded_target_key_returns_clear_error():
    mapping = AbInitioGraphParser().parse_file(FIXTURE)[0]
    rows = [
        {"ALERT_ID": "same", "CUSTOMER_ID": 12345},
        {"ALERT_ID": "same", "CUSTOMER_ID": 12345},
    ]

    with pytest.raises(ValueError, match="No non-null unique comparison key"):
        ValidationAgent._select_unique_comparison_keys(
            mapping,
            ["CUSTOMER_ID"],
            rows,
            rows,
            {"PROCESSED_ON"},
        )
