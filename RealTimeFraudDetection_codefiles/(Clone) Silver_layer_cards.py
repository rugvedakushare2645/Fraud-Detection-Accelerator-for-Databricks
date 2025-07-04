# Databricks notebook source
df_silver_layer = spark.read.format("parquet") \
            .option("header", "true") \
            .option("inferSchema","true") \
            .load("/Volumes/credit_card_schema/credit_fraud_volume/bronze_layer/part-00004-tid-1667414823475328605-96c8ed9a-1820-495f-8bf5-1917719a3524-142-1.c000.snappy.parquet")

display(df_silver_layer)

# COMMAND ----------

from pyspark.sql.functions import col

# Cast amount to float and is_fraud to integer
df_clean = df_silver_layer.withColumn("amount", col("amount").cast("float")) \
             .withColumn("is_fraud", col("is_fraud").cast("int"))

# Drop rows with critical nulls
df_clean = df_clean.dropna(subset=["transaction_id", "card_id", "amount", "timestamp", "is_fraud"])
df_clean.display()


# COMMAND ----------

# Drop rows with critical nulls
df_clean = df_clean.dropna(subset=["transaction_id", "card_id", "amount", "timestamp", "is_fraud"])
df_clean.display()

# COMMAND ----------

# from pyspark.sql.functions import (
#     col, when, hour, to_timestamp, unix_timestamp
# )

# # You can add this optional check to help debug
# required_cols = ["transaction_id", "amount", "timestamp", "is_fraud"]
# for colname in required_cols:
#     if colname not in df_clean.columns:
#         raise Exception(f"Missing required column: {colname}")

# df_features = (
#     df_clean
#     # Drop nulls in critical fields
#     .dropna(subset=["transaction_id", "amount", "timestamp", "is_fraud"])
#     # Remove duplicates
#     .dropDuplicates(["transaction_id"])
#     # Convert timestamp to usable formats
#     .withColumn("timestamp_ts", to_timestamp(col("timestamp")))
#     .withColumn("hour", hour(col("timestamp_ts")))
#     .withColumn("ts_unix", unix_timestamp(col("timestamp_ts")))
#     # Flag: large transaction (amount > 75000)
#     .withColumn("is_large_transaction", when(col("amount").cast("double") > 75000, 1).otherwise(0))
#     # Flag: night-time transaction (between 12AM–6AM)
#     .withColumn("is_night_transaction", when((col("hour") >= 0) & (col("hour") <= 6), 1).otherwise(0))
# )

# # Optional: Add these features only if the columns exist
# optional_columns = df_clean.columns

# if "country" in optional_columns:
#     df_features = df_features.withColumn(
#         "is_high_risk_country",
#         when(col("country").isin("GH", "CM", "NP", "HR", "KN"), 1).otherwise(0)
#     )

# # View final table
# df_features.select(
#     "transaction_id", "amount", "hour",
#     "is_night_transaction", "is_large_transaction", "is_fraud"
# ).display()


# COMMAND ----------

from pyspark.sql.functions import udf, col, expr
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import regexp_replace, col, to_timestamp

# Truncate microseconds to milliseconds (remove last 3 digits of microseconds)
df_clean = df_clean.withColumn(
    "timestamp_cleaned",
    regexp_replace(col("timestamp"), r"\.(\d{3})\d{3}", r".\1")
)

# Now safely convert to timestamp
df_clean = df_clean.withColumn(
    "timestamp_ts",
    to_timestamp(col("timestamp_cleaned"), "yyyy-MM-dd HH:mm:ss.SSS")
)




# UDFs
@udf(BooleanType())
def is_large_amount(amount):
    return amount > 300000

blacklisted_merchants = {"Vaughn, Cherry and Scott", "Bolton Ltd"}

@udf(BooleanType())
def is_blacklisted_merchant(merchant_name):
    return merchant_name in blacklisted_merchants

@udf(BooleanType())
def is_low_reputation(score):
    return score < 50

high_risk_countries = {"Uzbekistan", "Samoa", "Brunei Darussalam"}

@udf(BooleanType())
def is_high_risk_country(country):
    return country in high_risk_countries

@udf(BooleanType())
def is_suspicious_hour(ts):
    return ts is not None and (ts.hour < 6 or ts.hour > 22)

# Apply UDFs
df_flagged = df_clean \
    .withColumn("flag_large_amount", is_large_amount(col("amount"))) \
    .withColumn("flag_blacklisted_merchant", is_blacklisted_merchant(col("merchant_name"))) \
    .withColumn("flag_low_reputation", is_low_reputation(col("site_reputation_score").cast("float"))) \
    .withColumn("flag_high_risk_country", is_high_risk_country(col("country"))) \
    .withColumn("flag_suspicious_hour", is_suspicious_hour(col("timestamp_ts")))  # ✅ FIXED


# Add final fraud columns
df_with_risk = df_flagged \
    .withColumn("fraud_flag_count", expr(
        "int(flag_large_amount) + int(flag_blacklisted_merchant) + int(flag_low_reputation) + int(flag_high_risk_country) + int(flag_suspicious_hour)"
    )) \
    .withColumn("is_potential_fraud", expr("fraud_flag_count >= 3"))

# ✅ Now view the data
display(df_with_risk)


# COMMAND ----------

display(df_with_risk.filter("is_potential_fraud = true"))


# COMMAND ----------

display(df_with_risk)


# COMMAND ----------

bronze_path = "/Volumes/credit_card_schema/credit_fraud_volume/raw_data_files/transactions_enriched_delta"
silver_path = "/Volumes/credit_card_schema/credit_fraud_volume/silver_data/"

# COMMAND ----------

# Optionally clean the location
dbutils.fs.rm("/Volumes/credit_card_schema/credit_fraud_volume/silver_layer", True)

# Then write fresh
df_with_risk.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/credit_card_schema/credit_fraud_volume/silver_layer")


# COMMAND ----------

