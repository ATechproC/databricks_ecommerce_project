# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), False),
    StructField("country", StringType(), False),
    StructField("state", StringType(), False)
])
raw_data_path = "/Volumes/ecommerce/data_source/raw/customers/*.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path)
df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())
display(df.limit(10))

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_customers")