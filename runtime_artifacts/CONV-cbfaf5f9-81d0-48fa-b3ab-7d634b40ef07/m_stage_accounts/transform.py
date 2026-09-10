from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

def transform(input_df: DataFrame) -> DataFrame:
    return input_df.select(
        col("ACCOUNT_ID"),
        upper(col("ACCOUNT_STATUS")).alias("ACCOUNT_STATUS"),
        col("ACCOUNT_BALANCE")
    )
