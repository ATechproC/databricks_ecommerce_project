# Databricks notebook source
catalog_name = "ecommerce"

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_products")
display(df_silver.limit(10))

# COMMAND ----------

category_mapping = {
    "CE": "Electronics",
    "APP": "Apparel",
    "HNK": "Home & Kitchen",
    "BPC": "Beauty & Personal Care",
    "BKS": "Books",
    "GRCY": "Grocery",
    "TOY": "Toys & Games",
    "SPT": "Sports & Outdoors"
}

brand_mapping = {
    "ACME": "AcmeTech",
    "NOVW": "NovaWave",
    "ZNTH": "Zenith",
    "BYTM": "ByteMax",
    "ECOT": "EcoTone",
    "SKYL": "SkyLink",
    "VOLT": "VoltEdge",
    "PHTX": "Photonix",

    "URTL": "UrbanTrail",
    "COTC": "CottonClub",
    "FTFX": "FitFlex",
    "ABLF": "AmberLeaf",
    "MOSA": "Mosaic",
    "NTHR": "NorthThread",
    "CBLT": "CobaltWear",

    "CKMT": "CookMate",
    "HMNS": "HomeNest",
    "PRLV": "PureLiving",
    "CSNV": "CasaNova",
    "AQPR": "AquaPure",
    "STCR": "SteelCraft",
    "WRMH": "WarmHearth",

    "GLOW*": "GlowOn",
    "HRBC": "HerbaCare",
    "DMLX": "DermaLux",
    "SLKE": "SilkEssence",
    "ALMS": "AloeMist",
    "BTNQ": "Botaniq",

    "BLIN": "BlueInk",
    "PGTR": "PageTurner",
    "RDMR": "ReadMore",
    "QLHS": "QuillHouse",
    "STAC": "StoryArc",
    "LFSP": "Leaf&Spine",

    "FRFL": "FreshFields",
    "DLHV": "DailyHarvest",
    "GRBK": "GreenBasket",
    "NTCH": "NutriChoice",
    "GGRN": "GoodGrain",
    "FRMJ": "FarmJoy",

    "PLCB": "PlayCube",
    "HPFN": "HappyFun",
    "GMFG": "GameForge",
    "KDLN": "KiddoLand",
    "TNGR": "TinyGears",
    "RKBR": "RocketBear",

    "PRSD": "ProStride",
    "TRBZ": "TrailBlazer",
    "ARFT": "AeroFit",
    "PKPR": "PeakPro",
    "SMLN": "SummitLine",
    "SWCR": "SwiftCore"
}

# COMMAND ----------

df_gold = df_silver.replace(category_mapping, subset="category_code").replace(brand_mapping, subset="brand_code")
display(df_gold.limit(10))

# COMMAND ----------

rows, columns = df_gold.count(), len(df_gold.columns)
rows, columns

# COMMAND ----------

df_gold = df_gold.withColumnRenamed("brand_code", "brand_name").withColumnRenamed("category_code", "category_name")
display(df_gold.limit(10))

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_products")