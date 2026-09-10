import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

def test_filter_active_accounts(spark):
    schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_STATUS", StringType(), True),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    data = [
        (1, "ACTIVE", 1000.00),
        (2, "INACTIVE", 2000.00),
        (3, "ACTIVE", 3000.00),
        (4, None, 4000.00)
    ]

    df = spark.createDataFrame(data, schema)

    # Apply filter
    result_df = df.filter(df.ACCOUNT_STATUS == "ACTIVE")

    # Collect results
    results = result_df.collect()

    # Assert
    assert len(results) == 2
    assert results[0][0] == 1
    assert results[1][0] == 3
