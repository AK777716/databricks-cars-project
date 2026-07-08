# Azure End-to-End Data Engineering Project: Car Sales Analytics Pipeline

## 📌 Project Overview
This project implements an enterprise-grade cloud data solution that migrates transactional car sales data into a Medallion-structured Lakehouse. It features automated metadata-driven incremental data loading, data governance via Unity Catalog, Star Schema dimensional modeling, parallelized task orchestration, and SCD Type 1 upsert mechanics.

## 🏗️ Data Architecture Diagram
*(Pro-tip: Sketch a quick block diagram using Excalidraw or Draw.io using Azure/Databricks icons, save it as an image, upload it to this repository, and link it here!)*
![Architecture Diagram](your_uploaded_image_name.png)

---

## 🚀 Key Features & Implementation Details

### 1. Source System & Incremental Ingestion (ADF)
* **Data Source:** Azure SQL Database acting as the live transactional system.
* **Orchestration Pattern:** Implemented a metadata-driven high-watermark pattern utilizing twin lookup activities:
  * **Last Load Lookup:** Fetches the historical high-watermark checkpoint from a dedicated database logging table.
  * **Current Max Lookup:** Executes a dynamic `MAX(date_id)` query against the active transactional engine.
* **Delta Data Capture:** Programmed parameterized dynamic source expressions to parse and extract *only* newly created records matching interval thresholds.
* **Post-Execution Logging:** Triggers a transaction-safe stored procedure (`UpdateWatermarkTable`) upon ingestion success to commit the new execution threshold forward.

👉 `[Click here to view the pipeline JSON configurations](./adf/)`  
👉 `[Click here to view the staging records folder](./raw_data/)`

### 🗄️ Database State Control & Reset Automation
To handle development testing and absolute reproducibility, specialized database scripts were engineered to manage state:
* **Atomic State Updates:** Built a robust stored procedure encapsulated within explicit database transactions (`BEGIN TRANSACTION` and `COMMIT TRANSACTION`) to prevent logging drift.
* **Pipeline Repeatability:** Automated quick-reset environments utilizing staging `TRUNCATE` actions and state flushes (resetting criteria to `DT00000`). This allows the infrastructure to pivot cleanly between bulk historical initial runs and live incremental runs during data validation.

👉 `[Click here to view the complete SQL Checkpoint Setup Script](./database_scripts/watermark_control_setup.sql)`

### 2. Lakehouse Architecture & Security (Databricks + Unity Catalog)
* **Data Governance:** Abandoned legacy, insecure root DBFS directory mounting in favor of robust Unity Catalog configurations. Formed dedicated **External Locations** bound explicitly via an underlying **Azure Access Connector** managed identity.
* **Storage Optimization:** Landed incoming records utilizing highly compressed raw **Parquet** structures, transitioning into ACID-compliant **Delta Lake** formats for advanced data versioning, schema enforcement, and high-velocity upserts.

### 3. Transformation & Dimensional Modeling (Gold Layer)
* **Feature Engineering:** Leveraged PySpark (`withColumn`, `split`) to isolate hidden structural metadata from raw inputs (e.g., parsing hyphenated code layers to cleanly track high-level model categories).
* **Star Schema Realization:** Broke down flat transactional files into an optimized relational data model to drive business intelligence:
  * **Dynamic Surrogate Keys:** Calculated non-overlapping, unique surrogate constraint IDs on the fly by scanning existing maximum constraints during incremental streams.
  * **SCD Type 1 Enforcement:** Maintained reporting dimensions under clean **SCD Type 1 (Slowly Changing Dimensions)** configurations using targeted `DeltaTable.merge()` upsert conditions.
  * **Fact Table Alignment:** Formed a highly consolidated **Fact Sales Table** containing atomic measurements and operational indicators paired tightly with downstream dimension keys.

👉 `[Click here to view the PySpark & SQL notebooks](./databricks_notebooks/)`

---

## 🔄 Workflow Orchestration & Dependency Graph

The entire downstream pipeline is fully automated and performance-optimized via **Databricks Workflows**. To drastically minimize cluster runtime and execution costs, the dimensional layers spin up concurrently rather than waiting on a traditional sequential loop.

![Databricks Workflow Dependency Graph](./execution_snapshots/databricks_workflow.png)

### Execution Sequence:
1. **Silver Transformation Data Layer:** The ingestion engine kicks off the `silver_notebook` to parse raw fields, structure metrics, and write to Silver Storage.
2. **Parallel Dimension Processing:** Upon successful write to Silver, 4 independent tasks (`Dim_Branch`, `Dim_Date`, `Dim_Dealer`, and `Dim_Model`) initiate simultaneously to run their respective delta constraints and update Lookups in parallel.
3. **Gold Fact Integration:** The terminal process, `fact_Sales`, evaluates upstream conditions and triggers immediately upon successful completion of all four dimension tasks to aggregate final reporting tables.

---

## 📈 Final Insights & Execution Snapshots

Recruiters and data consumers can track the exact runtime behaviors, visualization charts, schema printouts, and system query parameters generated by the clusters without needing an active Azure subscription. 

👉 `[Click here to explore the interactive cell output files](./execution_snapshots/)`