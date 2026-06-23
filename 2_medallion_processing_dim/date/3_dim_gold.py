# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_date")
display(df_silver.limit(10))

# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd"))

# COMMAND ----------

df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# COMMAND ----------

display(df_gold.limit(10))

# COMMAND ----------

df_gold = df_gold.withColumn(
    "isWeekend", 
    F.when(F.col("day_name").isin("Sunday", "Saturday"), 1).otherwise(0)
)
display(df_gold.limit(10))

# COMMAND ----------

columns_order = ["date_id", "date", "year", "day_name", "isWeekend", "month_name", "quarter", "week_of_year","_source_file", "_ingested_at"]
df_gold = df_gold.select(columns_order)
display(df_gold.limit(10))

# COMMAND ----------

df_gold.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_date")