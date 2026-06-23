# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_order_items")

# COMMAND ----------

df_silver = df_bronze.withColumn("dt", F.to_date(F.col("dt"), "yyyy-MM-dd"))

# COMMAND ----------

df_silver = df_silver.withColumn("order_ts", F.coalesce(
    F.to_timestamp(F.col("order_ts"), "yyyy-MM-dd HH:mm:ss"),
    F.to_timestamp(F.col("order_ts"), "dd-MM-yyyy HH:mm")
))

# COMMAND ----------

df_silver = df_silver.withColumn(
    "quantity",
    F.when(F.col("quantity") == "One", 1)
     .when(F.col("quantity") == "Two", 2)
     .otherwise(F.col("quantity")).cast("int")
)

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["dt"])
df_silver.count()

# COMMAND ----------

df_silver.groupBy("dt").count().filter(F.col("count")>1).show()

# COMMAND ----------

display(df_bronze.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("unit_price", F.regexp_replace(F.col("unit_price"), "[^0-9]", "").cast("double"))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("discount_pct", F.regexp_replace(F.col("discount_pct"), "[^0-9]", "").cast("double"))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("tax_amount", F.regexp_replace(F.col("tax_amount"), "[^0-9]", "").cast("double"))
display(df_silver.limit(10))

# COMMAND ----------

channels = {
    "web" : "Website",
    "app" : "Mobile"
}

df_silver = df_silver.replace(channels, subset="channel")
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("item_seq", F.col("item_seq").cast("int"))
display(df_silver.limit(10))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_order_items")