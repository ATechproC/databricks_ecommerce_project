# Databricks notebook source
from pyspark.sql.types import StringType, StructType, StructField
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

order_items_schema = StructType([
    StructField("dt",                 StringType(), True),
    StructField("order_ts",           StringType(), True),
    StructField("customer_id",        StringType(), True),
    StructField("order_id",           StringType(), True),
    StructField("item_seq",           StringType(), True),
    StructField("product_id",         StringType(), True),
    StructField("quantity",           StringType(), True),
    StructField("unit_price_currency",StringType(), True),
    StructField("unit_price",         StringType(), True),
    StructField("discount_pct",       StringType(), True),
    StructField("tax_amount",         StringType(), True),
    StructField("channel",            StringType(), True),
    StructField("coupon_code",        StringType(), True),
])

raw_data_path = "/Volumes/ecommerce/data_source/raw/order_items/landing/*.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", ",").schema(order_items_schema).csv(raw_data_path)
df = df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("mergeSchma", "true").saveAsTable(f"{catalog_name}.bronze.brz_order_items")