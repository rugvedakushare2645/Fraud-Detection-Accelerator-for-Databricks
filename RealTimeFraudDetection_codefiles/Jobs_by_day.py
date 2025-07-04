# Databricks notebook source
dbutils.widgets.text("input_date","")

# COMMAND ----------

_input_date = dbutils.widgets.get("input_date")
print(_input_date)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

_input_day = spark.sql(f"""
                        select date_format(to_timestamp('{_input_date}',"yyyy-MM-dd'T'HH:mm:ss"), 'E')
                       """).collect()[0][0]
print(_input_day)


# COMMAND ----------

dbutils.jobs.taskValues.set("input_day", _input_day)