# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
display(df_silver)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

anomalies = {
    "BOOKS" : "BKS",
    "GROCERY" : "GRCY",
    "TOYS" : "TOY"
}

# COMMAND ----------

df_silver = df_silver.replace(anomalies, subset="category_code")
display(df_silver)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_brands")