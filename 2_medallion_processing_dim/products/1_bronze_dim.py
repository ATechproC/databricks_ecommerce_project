# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  #datatype is string due to incoming data contain anamolies
    StructField("length_cm", StringType(), True),     #datatype is string due to incoming data contain anamolies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True)
])

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/data_source/raw/products/*.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path)

# COMMAND ----------

df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_products")