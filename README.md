# 🛒 E-commerce Data Engineering Project with Databricks

## 📌 Overview

This project focuses on building an **E-commerce Data Engineering Platform using Databricks and PySpark**.

The objective is to develop a structured data pipeline that ingests raw e-commerce data, applies data cleaning and transformation processes, and prepares analytics-ready datasets for business intelligence and reporting.

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** and uses a **Star Schema** in the Gold layer for business and analytical usage.

---

## 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │    Raw CSV Files     │
                    │   E-commerce Data    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Bronze Layer      │
                    │   Raw Data Ingestion │
                    │      Delta Tables    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │     Silver Layer    │
                    │  Cleaning & Quality  │
                    │  Standardization     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Gold Layer      │
                    │   Business-Ready     │
                    │     Star Schema      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Business Intelligence│
                    │   Reporting &        │
                    │      Analytics       │
                    └──────────────────────┘
```

---

## 🛠️ Technologies Used

* **Databricks** — Data engineering and processing platform
* **Apache Spark / PySpark** — Distributed data processing
* **Delta Lake** — Storage format for data tables
* **Python** — Data transformations and pipeline logic
* **SQL** — Catalog and schema management
* **Unity Catalog** — Data organization and governance

---

## 🗂️ Project Structure

```text
databricks_ecommerce_project/
│
├── 1_setup/
│   └── setup.py
│
├── 2_medallion_processing_dim/
│   ├── brands/
│   │   ├── 1_bronze_dim.py
│   │   └── 2_silver_dim.py
│   │
│   ├── category/
│   ├── customers/
│   ├── date/
│   └── products/
│
├── 2_medallion_processing_fact/
│   └── order_items/
│       ├── 1_bronze_fact.py
│       ├── 2_silver_fact.py
│       └── 3_gold_fact.py
│
├── ecomm-raw-data/
│   ├── brands/
│   ├── category/
│   ├── customers/
│   ├── date/
│   ├── order_items/
│   └── products/
│
└── README.md
```

---

## ⚙️ Data Pipeline

### 1. Setup Layer

The project starts by creating the Databricks catalog and schemas:

```sql
CREATE CATALOG IF NOT EXISTS ecommerce;

USE CATALOG ecommerce;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

This provides a structured environment for storing data across the different processing layers.

---

### 2. Bronze Layer — Raw Data Ingestion

The Bronze layer is responsible for ingesting raw CSV files into Delta tables.

#### Example: Brands Dataset

Source:

```text
/Volumes/ecommerce/data_source/raw/brands/*.csv
```

Target table:

```text
ecommerce.bronze.brz_brands
```

#### Main Operations

* Define an explicit PySpark schema.
* Read CSV files using Spark.
* Track the source file path.
* Add an ingestion timestamp.
* Store the data as a Delta table.

Example metadata columns:

```text
_source_file
_ingested_at
```

These columns help track where data came from and when it was ingested.

---

### 3. Silver Layer — Data Cleaning & Transformation

The Silver layer transforms raw data into cleaner and more consistent datasets.

#### Example: Brands Transformation

Source table:

```text
ecommerce.bronze.brz_brands
```

Target table:

```text
ecommerce.silver.slv_brands
```

#### Transformations Implemented

**Brand name cleaning**

Remove unnecessary spaces using PySpark:

```python
df_silver = df_bronze.withColumn(
    "brand_name",
    F.trim(F.col("brand_name"))
)
```

**Brand code standardization**

Remove unwanted characters from brand codes:

```python
df_silver = df_silver.withColumn(
    "brand_code",
    F.regexp_replace(
        F.col("brand_code"),
        r'[^A-Za-z0-9]',
        ''
    )
)
```

**Category code standardization**

Handle inconsistent category codes using a mapping dictionary:

```python
anomalies = {
    "BOOKS": "BKS",
    "GROCERY": "GRCY",
    "TOYS": "TOY"
}
```

This helps maintain consistency across the dataset and supports reliable downstream analytics.

---

### 4. Gold Layer — Business-Ready Data

The Gold layer is designed for **business usage, analytics, and reporting**.

The project uses a **Star Schema** to organize e-commerce data into fact and dimension tables.

#### ⭐ Fact Tables

Fact tables contain measurable business events and metrics.

Example:

```text
ecommerce.gold.fact_order_items
```

Potential measures include:

* Quantity
* Unit price
* Discount percentage
* Tax amount
* Sales amount

#### ⭐ Dimension Tables

Dimension tables provide descriptive information about business entities.

Examples:

```text
ecommerce.gold.dim_products
ecommerce.gold.dim_brands
ecommerce.gold.dim_customers
ecommerce.gold.dim_category
ecommerce.gold.dim_date
```

The Star Schema is intended to make analytical queries easier and support business intelligence use cases.

---

## 🎯 Business Use Cases

The platform is designed to support:

* E-commerce sales analysis
* Product performance analysis
* Customer purchasing behavior
* Brand and category analysis
* Revenue and order-item reporting
* Business intelligence dashboards

---

## 📚 Key Learning Outcomes

Through this project, I am developing practical experience with:

* Building data pipelines using Databricks.
* Working with PySpark DataFrames.
* Implementing Medallion Architecture.
* Using Delta Lake tables.
* Applying data cleaning and standardization.
* Organizing data with Unity Catalog.
* Designing analytical data models using Star Schema.
* Preparing data for business reporting.

---

## 🚀 Future Improvements

* Implement incremental data processing.
* Add data quality checks.
* Introduce pipeline orchestration.
* Improve error handling and monitoring.
* Optimize Delta tables.
* Connect the Gold layer to a BI tool.

---

## 👨‍💻 Author

**Anass CHORAICHI**

Aspiring Data Engineer | Python | SQL | PySpark | Databricks

---

## 🔗 Repository

[GitHub Repository](https://github.com/ATechproC/databricks_ecommerce_project)
