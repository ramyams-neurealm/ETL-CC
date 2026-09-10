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
raw_account_df = spark.read.format("jdbc").options(
    url="jdbc:oracle:thin:@//<HOST>:<PORT>/<SERVICE>",
    dbtable="RAW_ACCOUNT",
    user="<USERNAME>",
    password="<PASSWORD>"
).schema(raw_account_schema).load()

# Apply transformation: EXP_STANDARDIZE_ACCOUNT
exp_standardize_account_df = raw_account_df.select(
    raw_account_df.ACCOUNT_ID,
    upper(raw_account_df.ACCOUNT_STATUS).alias("ACCOUNT_STATUS"),
    raw_account_df.ACCOUNT_BALANCE
)

# Write to STG_ACCOUNT
exp_standardize_account_df.write.format("jdbc").options(
    url="jdbc:oracle:thin:@//<HOST>:<PORT>/<SERVICE>",
    dbtable="STG_ACCOUNT",
    user="<USERNAME>",
    password="<PASSWORD>"
).mode("overwrite").save()
