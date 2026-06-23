# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_date")
display(df_bronze.limit(10))

# COMMAND ----------

rows, columns = df_bronze.count(), len(df_bronze.columns)
rows, columns

# COMMAND ----------

df_silver = df_bronze.withColumn("date", F.to_date(F.col("date"), "dd-MM-yyyy"))

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

df_silver.groupBy("date").agg(
    F.count("date").alias("cnt")
).filter(F.col("cnt")>1).show()

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["date"])

# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("-", F.concat(F.lit("Q"),F.col("quarter")), F.col("year")))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year")), F.col("year")))

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_date")