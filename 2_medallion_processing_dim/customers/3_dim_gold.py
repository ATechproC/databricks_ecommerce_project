# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_customers")
display(df_silver.limit(10))

# COMMAND ----------

rows, columns = df_silver.count(), len(df_silver.columns)

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# COMMAND ----------

country_state_map

# COMMAND ----------

rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))

# COMMAND ----------

rows[:10]

# COMMAND ----------

df_regions = spark.createDataFrame(rows)
display(df_regions)

# COMMAND ----------

df_silver_cusotmers = spark.read.table(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

df_gold = df_silver_cusotmers.join(df_regions, on=["country", "state"])

# COMMAND ----------

df_gold.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_customers")