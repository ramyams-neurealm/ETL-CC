import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal
from transform import transform

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_transform(spark):
    schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), nullable=False),
        StructField("ACCOUNT_STATUS", StringType(), nullable=True),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), nullable=True)
    ])

    input_data = [
        (1, "active", Decimal("1000.00")),
        (2, "inactive", Decimal("200.50")),
        (3, None, None)
    ]

    expected_data = [
        (1, "ACTIVE", Decimal("1000.00")),
        (2, "INACTIVE", Decimal("200.50")),
        (3, None, None)
    ]

    input_df = spark.createDataFrame(input_data, schema)
    expected_df = spark.createDataFrame(expected_data, schema)

    result_df = transform(input_df)

    assert result_df.collect() == expected_df.collect()
