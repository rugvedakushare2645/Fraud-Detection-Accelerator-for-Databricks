# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/credit_card_schema/credit_fraud_volume/silver_layer/`
# MAGIC

# COMMAND ----------

df_gold = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/Volumes/credit_card_schema/credit_fraud_volume/silver_layer/")
display(df_gold)

# COMMAND ----------

df_gold.count()


# COMMAND ----------

df_dashboard = (df_gold.filter("is_potential_fraud = true"))


# COMMAND ----------

df_gold.write.mode("overwrite").format("delta").save("/Volumes/credit_card_schema/credit_fraud_volume/gold_layer")


# COMMAND ----------

display(df_dashboard)

# COMMAND ----------

df_dashboard.write.mode("overwrite").format("delta").save("/Volumes/credit_card_schema/credit_fraud_volume/gold_layer_dashboard")
