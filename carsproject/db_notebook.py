# Databricks notebook source
spark

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@carakanshdatalake.dfs.core.windows.net/"))

# COMMAND ----------

spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW STORAGE CREDENTIALS").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE EXTERNAL LOCATION bronzeext").show(truncate=False)

# COMMAND ----------

spark.sql("SELECT current_schema()").show()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC In this new version of databrics a default catlog is alredy created.
# MAGIC we can create new catlogs in same workspce but here we are sticking to the default .
# MAGIC we will create schemas in it only

# COMMAND ----------

# MAGIC %md
# MAGIC # create schemas
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cardatabricks2.silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cardatabricks2.gold

# COMMAND ----------

