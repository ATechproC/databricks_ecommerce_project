# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")
display(df_bronze)

# COMMAND ----------

df_bronze.select("category_code").distinct().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category_code, count(category_code) as CountPerCategory
# MAGIC FROM ecommerce.bronze.brz_category
# MAGIC group by category_code
# MAGIC HAVING CountPerCategory > 1;

# COMMAND ----------

df_bronze.groupBy("category_code").count().filter(F.col("count") > 1).show()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["category_code"])

# COMMAND ----------

df_silver.groupBy("category_code").agg(
    F.count("category_code").alias("cnt")
).show()

# COMMAND ----------

df_silver = df_bronze.withColumn("category_code", F.upper(F.col("category_code")))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_category")