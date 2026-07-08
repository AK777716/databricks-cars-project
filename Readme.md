# Azure End-to-End Data Engineering Project: Car Sales Analytics Pipeline

## 📌 Project Overview
This project implements an enterprise-grade cloud data solution that migrates transactional car sales data into a Medallion-structured Lakehouse. It features automated incremental data loading, data governance via Unity Catalog, Star Schema dimensional modeling, and SCD Type 1 upsert mechanics.

## 🏗️ Data Architecture Diagram
*(Pro-tip: Sketch a quick block diagram using Excalidraw or Draw.io using Azure/Databricks icons, save it as an image, upload it to this folder, and link it here!)*
`![Architecture Diagram](your_uploaded_image_name.png)`

## 🚀 Key Features & Implementation Details

### 1. Source System & Incremental Ingestion (ADF)
* **Data Source:** Azure SQL Database acting as the transactional system.
* **Orchestration Pattern:** Implemented a metadata-driven Watermark pattern utilizing two lookup activities.
  * **Last Load Lookup:** Fetches the checkpoint high-watermark from a dedicated control table.
  * **Current Max Lookup:** Executes a dynamic `MAX(date_id)` calculation against the live transactional system.
* **Delta Capture:** Programmed the parameterized dynamic source filter expression to extract *only* newly created delta records between pipeline intervals.
* **Post-Execution Logging:** Triggers a stored procedure on pipeline success to securely commit the new execution high-watermark.

### 2. Lakehouse Architecture & Security (Databricks + Unity Catalog)
* **Data Governance:** Abandoned legacy root DBFS mounting in favor of Unity Catalog. Configured dedicated **External Locations** bound securely via an **Azure Access Connector**.
* **Storage Engine:** Landed raw data layers utilizing compressed **Parquet** formats, transitioning to **Delta Lake** for advanced storage management.

### 3. Transformation & Dimensional Modeling (Gold Layer)
* **Feature Engineering:** Leveraged PySpark (`withColumn`, `split`) to cleanly extract structural metadata (e.g., parsing hyphenated strings to isolate clear model categories).
* **Star Schema Realization:** Hand-crafted a high-performance Dimensional Model.
  * Calculated distinct, non-overlapping surrogate keys dynamically by checking current maximum constraints on existing dimensions during live increment execution.
  * Enforced **SCD Type 1 (Slowly Changing Dimensions)** behavior across reporting entities utilizing targeted `DeltaTable.merge()` mechanics.
  * Created a consolidated **Fact Sales Table** optimizing atomic measure transactions mapped precisely against dimension surrogate keys.

### 4. Workflow Orchestration
The pipeline is fully automated via **Databricks Workflows**, processing transformations concurrently:
1. Ingest & Transform Raw Data -> Write to Silver.
2. Spin up 4 concurrent dimension streams (`dim_model`, `dim_branch`, `dim_dealer`, `dim_date`) to refresh lookups in parallel.
3. Compute the final downstream dependency, executing `gold_fact_sales` on dimension completion.

## 📈 Final Insights & Reports
*(Optional: If you successfully connected to Power BI or generated graphs inside Databricks, drop a screenshot right here to prove the data is actionable!)*