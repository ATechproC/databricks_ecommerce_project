# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")
display(df_bronze.limit(10))

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code"))).withColumn("brand_code", F.upper(F.col("brand_code")))

# COMMAND ----------

df_silver = df_silver.withColumn("material", F.initcap(F.col("material")))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("weight_grams", F.regexp_replace(F.col("weight_grams"),"g","").cast(IntegerType()))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn("length_cm",                             
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType()))
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
    .otherwise(F.lit(0))
)
display(df_silver.limit(10))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.silver.slv_products")