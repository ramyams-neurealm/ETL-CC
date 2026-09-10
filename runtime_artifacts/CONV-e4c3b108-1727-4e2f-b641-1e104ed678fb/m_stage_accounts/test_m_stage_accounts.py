import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import upper

class TestMStageAccounts(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("test_m_stage_accounts").getOrCreate()
        
        # Define schema for RAW_ACCOUNT
        self.raw_account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), nullable=False),
            StructField("ACCOUNT_STATUS", StringType(), nullable=True),
            StructField("ACCOUNT_BALANCE", DecimalType(15, 2), nullable=True)
        ])

    def test_exp_standardize_account(self):
        # Create a sample dataframe
        data = [
            (1, "active", 1000.00),
            (2, "inactive", 2000.50),
            (3, None, 3000.75)
        ]
        raw_account_df = self.spark.createDataFrame(data, schema=self.raw_account_schema)

        # Expected data after transformation
        expected_data = [
            (1, "ACTIVE", 1000.00),
            (2, "INACTIVE", 2000.50),
            (3, None, 3000.75)
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=self.raw_account_schema)

        # Apply transformation
        result_df = raw_account_df.select(
            raw_account_df.ACCOUNT_ID,
            upper(raw_account_df.ACCOUNT_STATUS).alias("ACCOUNT_STATUS"),
            raw_account_df.ACCOUNT_BALANCE
        )

        # Collect results
        result_data = result_df.collect()
        expected_data = expected_df.collect()

        # Assert results
        self.assertEqual(result_data, expected_data)

    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()
