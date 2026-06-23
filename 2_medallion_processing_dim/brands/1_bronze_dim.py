# Databricks notebook source
from pyspark.sql.types import StructField, StringType, StructType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), False),
    StructField("category_code", StringType(), False)
])

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/data_source/raw/brands/*.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

# COMMAND ----------

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_brands")