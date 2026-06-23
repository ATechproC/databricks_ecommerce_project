# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

date_schema = StructType([
    StructField("date", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("day_name", StringType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("week_of_year", IntegerType(), False)
])

raw_data_path = "/Volumes/ecommerce/data_source/raw/date/*.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path)
df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_date")