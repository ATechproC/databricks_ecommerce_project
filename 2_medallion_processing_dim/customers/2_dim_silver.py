# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")
display(df_bronze.limit(10))

# COMMAND ----------

null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

rows, columns = df_bronze.count(), len(df_bronze.columns)
rows, columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows from ecommerce.bronze.brz_customers

# COMMAND ----------

df_silver = df_bronze.dropna(subset=["customer_id"])

# COMMAND ----------

null_count = df_silver.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

df_silver.filter(F.col("phone").isNull()).show(3)

# COMMAND ----------

df_silver = df_silver.fillna("Not Available", subset=["phone"])

# COMMAND ----------

df_silver.filter(F.col("phone").isNull()).show(3)

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_customers")