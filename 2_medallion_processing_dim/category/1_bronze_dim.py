# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), False)
])

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/data_source/raw/category/*.csv"

# COMMAND ----------

df= spark.read.option("header", "true").option("delimiter", ",").csv(raw_data_path)
df = df_bronze.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_category")