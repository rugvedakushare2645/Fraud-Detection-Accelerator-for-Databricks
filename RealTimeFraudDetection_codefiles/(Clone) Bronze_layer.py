# Databricks notebook source
from pyspark.sql.functions import col

raw_path = "/Volumes/credit_card_schema/credit_fraud_volume/raw_data_files/"
bronze_path = "/Volumes/credit_card_schema/credit_fraud_volume/bronze_layer/"
# Load all base DataFrames from Bronze CSVs
transactions       = spark.read.option("header", True).csv(raw_path + "transactions.csv")
cards              = spark.read.option("header", True).csv(raw_path + "cards.csv")
websites           = spark.read.option("header", True).csv(raw_path + "websites.csv")
locations          = spark.read.option("header", True).csv(raw_path + "locations.csv")
devices            = spark.read.option("header", True).csv(raw_path + "devices.csv")
ips                = spark.read.option("header", True).csv(raw_path + "ips.csv")
transaction_types  = spark.read.option("header", True).csv(raw_path + "transaction_types.csv")
banks              = spark.read.option("header", True).csv(raw_path + "banks.csv")
fraud_labels       = spark.read.option("header", True).csv(raw_path + "fraud_labels.csv")
merchants          = spark.read.option("header", True).csv(raw_path + "merchants.csv")

# Join sequence
bronze_enriched = (
    transactions
    .join(cards, "card_id", "left")
    .join(banks, "bank_id", "left")
    .join(merchants, "merchant_id", "left")
    .join(websites, "website_id", "left")
    .join(locations, "location_id", "left")
    .join(devices, "device_id", "left")
    .join(ips, "ip_id", "left")
    .join(transaction_types, "type_id", "left")
    .join(fraud_labels, "is_fraud", "left")
)

# Save enriched bronze table as a Delta table in bronze volume
#bronze_enriched.write.mode("overwrite").format("delta").save(bronze_path + "transactions_enriched_delta")
bronze_enriched.write.format("parquet") \
    .mode("append") \
    .option("path", "/Volumes/credit_card_schema/credit_fraud_volume/bronze_layer") \
    .save()
print("✅ Joined and saved enriched bronze table as Delta at:")
print(bronze_path + "transactions_enriched_delta")


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/credit_card_schema/credit_fraud_volume/raw_data_files/"))



# COMMAND ----------

bronze_enriched.count()

# COMMAND ----------

display(bronze_enriched)