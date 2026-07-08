# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC Reading Silver Data

# COMMAND ----------

df_silver = spark.sql("""
SELECT *
FROM parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales`
""")

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading all the Dims

# COMMAND ----------

df_dealer=spark.sql("Select * from cardatabricks2.gold.dim_dealer")
df_branch=spark.sql("Select * from cardatabricks2.gold.dim_branch")
df_model=spark.sql("Select * from cardatabricks2.gold.dim_model")
df_date=spark.sql("Select * from cardatabricks2.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bringing keys to the fact table

# COMMAND ----------

df_fact = (
    df_silver
    .join(df_branch, df_silver["Branch_ID"] == df_branch["Branch_ID"], "left")
    .join(df_dealer, df_silver["Dealer_ID"] == df_dealer["Dealer_ID"], "left")
    .join(df_model, df_silver["Model_ID"] == df_model["Model_ID"], "left")
    .join(df_date, df_silver["Date_ID"] == df_date["Date_ID"], "left")
    .select(
        df_silver["Revenue"],
        df_silver["Units_Sold"],
        df_silver["RevenuePerUnit"],
        df_dealer["dim_dealer_key"],
        df_branch["dim_branch_key"],
        df_model["dim_model_key"],
        df_date["dim_date_key"]
    )
)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Fact Table

# COMMAND ----------

from delta import tables

# COMMAND ----------

if spark.catalog.tableExists("factsales"):

    delta_tbl = DeltaTable.forPath(
        spark,
        "abfss://gold@carakanshdatalake.dfs.core.windows.net/factsales"
    )

    (
        delta_tbl.alias("trg")
        .merge(
            df_fact.alias("src"),
            """
            trg.dim_branch_key = src.dim_branch_key
            AND trg.dim_dealer_key = src.dim_dealer_key
            AND trg.dim_model_key = src.dim_model_key
            AND trg.dim_date_key = src.dim_date_key
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("path", "abfss://gold@carakanshdatalake.dfs.core.windows.net/factsales")
        .saveAsTable("cardatabricks2.gold.factsales")
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cardatabricks2.gold.factsales

# COMMAND ----------

