from pathlib import Path

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.agents.conversion_agent import ConversionAgent


FIXTURE = (
    Path(__file__).parents[1]
    / "runtime_sources"
    / "ab_initio_graph"
    / "sample_ab_initio_graph.mp"
)


def _mapping():
    return AbInitioGraphParser().parse_file(FIXTURE)[0]


def test_pyspark_contract_requires_none_safe_parameters_and_target_names():
    content = """
from pyspark.sql import functions as F

def transform(inputs, *, parameters=None):
    threshold_amt = parameters.get("THRESHOLD_AMT", 10000.00)
    txn_df = inputs["SRC_TRANSACTIONS"]
    return txn_df.filter(F.col("TXN_AMOUNT") >= threshold_amt).select(
        F.col("TXN_ID").alias("ALERT_ID"),
        F.col("CUSTOMER_ID"),
        F.col("TXN_AMOUNT"),
        F.current_date().alias("PROCESSED_ON"),
    )
"""

    errors = ConversionAgent()._validate_pyspark_contract(content, _mapping())

    assert any("normalize its optional parameters" in error for error in errors)
    assert any("ALERT_AMOUNT" in error for error in errors)
    assert errors == [
        error
        for error in errors
        if "normalize its optional parameters" in error
        or "ALERT_AMOUNT" in error
    ]


def test_pyspark_contract_accepts_safe_parameters_and_aliases_target_field():
    content = """
from pyspark.sql import functions as F

def transform(inputs, *, parameters=None):
    parameters = parameters or {}
    threshold_amt = parameters.get("THRESHOLD_AMT", 10000.00)
    proc_date = parameters.get("PROC_DATE")
    txn_df = inputs["SRC_TRANSACTIONS"]
    return txn_df.filter(F.col("TXN_AMOUNT") >= threshold_amt).select(
        F.concat(F.lit("ALRT_"), F.col("TXN_ID")).alias("ALERT_ID"),
        F.col("CUSTOMER_ID"),
        F.col("TXN_AMOUNT").alias("ALERT_AMOUNT"),
        F.lit(proc_date).cast("date").alias("PROCESSED_ON"),
    )
"""

    errors = ConversionAgent()._validate_pyspark_contract(content, _mapping())

    assert errors == []
