from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("m_load_active_accounts").getOrCreate()

# Load source data
stg_account_df = spark.read.format("jdbc").options(
    url="jdbc:oracle:thin:@//hostname:port/service_name",
    dbtable="STAGE.STG_ACCOUNT",
    user="username",
    password="password"
).load()

# Apply filter transformation
fil_active_accounts_df = stg_account_df.filter(col("ACCOUNT_STATUS") == "ACTIVE")

# Select required columns for target
tgt_active_account_df = fil_active_accounts_df.select(
    col("ACCOUNT_ID"),
    col("ACCOUNT_BALANCE")
)

# Write to target
# Note: Replace with actual target configuration
# tgt_active_account_df.write.format("jdbc").options(
#     url="jdbc:oracle:thin:@//hostname:port/service_name",
#     dbtable="TARGET.TGT_ACTIVE_ACCOUNT",
#     user="username",
#     password="password"
# ).mode("overwrite").save()
