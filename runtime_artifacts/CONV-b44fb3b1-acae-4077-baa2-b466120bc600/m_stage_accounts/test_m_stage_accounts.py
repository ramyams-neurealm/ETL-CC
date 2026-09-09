import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import upper

class TestMStageAccounts(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("test_m_stage_accounts").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exp_standardize_account(self):
        schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), nullable=False),
            StructField("ACCOUNT_STATUS", StringType(), nullable=True),
            StructField("ACCOUNT_BALANCE", DecimalType(15, 2), nullable=True)
        ])

        data = [
            (1, "active", 1000.00),
            (2, "inactive", 2000.50),
            (3, None, 3000.75)
        ]

        raw_account_df = self.spark.createDataFrame(data, schema)

        expected_data = [
            (1, "ACTIVE", 1000.00),
            (2, "INACTIVE", 2000.50),
            (3, None, 3000.75)
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema)

        result_df = raw_account_df.withColumn("ACCOUNT_STATUS", upper(raw_account_df["ACCOUNT_STATUS"]))

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

if __name__ == '__main__':
    unittest.main()
