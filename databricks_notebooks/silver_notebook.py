# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading
# MAGIC

# COMMAND ----------

df=spark.read.format('parquet')\
     .option('inferschema',True)\
         .load('abfss://bronze@carakanshdatalake.dfs.core.windows.net/raw_data')


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL cardatabricks2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION bronzeext;

# COMMAND ----------

# MAGIC %sql SHOW GRANTS ON EXTERNAL LOCATION bronzeext;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES
# MAGIC ON EXTERNAL LOCATION bronzeext
# MAGIC TO `akansh.maroli235@svkmmumbai.onmicrosoft.com`;

# COMMAND ----------

spark.sql("SHOW GRANTS ON EXTERNAL LOCATION bronzeext")

# COMMAND ----------

df = spark.read.parquet(
    "abfss://bronze@carakanshdatalake.dfs.core.windows.net/raw_data"
)

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION bronzeext;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION bronzeext;

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@carakanshdatalake.dfs.core.windows.net/")

# COMMAND ----------

spark

# COMMAND ----------

df = spark.read.parquet(
    "abfss://bronze@carakanshdatalake.dfs.core.windows.net/raw_data"
)

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CURRENT SCHEMA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL cardatabricks2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL carstoragecred;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN cardatabricks2.default;

# COMMAND ----------

spark

# COMMAND ----------

df=spark.read.format('parquet')\
     .option('inferschema',True)\
         .load('abfss://bronze@carakanshdatalake.dfs.core.windows.net/raw_data')

# COMMAND ----------

df.display()

# COMMAND ----------

print(df.count())

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df=df.withColumn('model_category',split(col('model_id'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

df=df.withColumn('RevenuePerUnit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AD-HOC
# MAGIC

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).orderBy('Year','Total_Units', ascending=[True, False]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales')\
    .save()

     

# COMMAND ----------

# MAGIC %md
# MAGIC #     Querying SilverData

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales`

# COMMAND ----------

