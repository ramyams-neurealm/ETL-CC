from pyspark.sql import SparkSession
from pyspark.sql.functions import upper
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType

# Initialize Spark session
spark = SparkSession.builder.appName("m_stage_accounts").getOrCreate()

# Define schema for RAW_ACCOUNT
raw_account_schema = StructType([
    StructField("ACCOUNT_ID", IntegerType(), nullable=False),
    StructField("ACCOUNT_STATUS", StringType(), nullable=True),
    StructField("ACCOUNT_BALANCE", DecimalType(15, 2), nullable=True)
])

# Load RAW_ACCOUNT data
raw_account_df = spark.read.format("parquet").schema(raw_account_schema).load("/path/to/raw_account")

# Apply transformation: EXP_STANDARDIZE_ACCOUNT
exp_standardize_account_df = raw_account_df \
    .withColumn("ACCOUNT_STATUS", upper(raw_account_df["ACCOUNT_STATUS"]))

# Write to STG_ACCOUNT
exp_standardize_account_df.write.format("parquet").mode("overwrite").save("/path/to/stg_account")

spark.stop()
