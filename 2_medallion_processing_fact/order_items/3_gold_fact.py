# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce.silver.slv_order_items limit 10

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.slv_order_items")
display(df.limit(10))

# COMMAND ----------

# 1) Add gross amount
df_gold = df.withColumn(
    "gross_amount",
    F.col("quantity") * F.col("unit_price")
    )

# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df_gold = df_gold.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

# 3) Add sale_amount = gross - discount
df_gold = df_gold.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

# add date id
df_gold = df_gold.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df_gold = df_gold.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)

display(df_gold.limit(10))   

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_fact_order_items)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")