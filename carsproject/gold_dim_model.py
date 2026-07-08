# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*


# COMMAND ----------

# MAGIC %md
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text("incremental_flag",'0')

# COMMAND ----------

incremental_flag =dbutils.widgets.get("incremental_flag")
print(incremental_flag)
  

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Fetch Relative Columns 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Model_Id)as Model_Id ,model_category from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales`

# COMMAND ----------

df_src=spark.sql("""
   select Distinct(Model_Id) as Model_Id, model_category from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   """)
   

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model Sink-Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists("cardatabricks2.gold.dim_model"):
    df_sink=spark.sql("""
   select  dim_model_key, Model_Id, model_category from cardatabricks2.gold.dim_model 
  
   """)


else:
    df_sink=spark.sql("""
   select 1 as dim_model_key, Model_Id, model_category from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   where 1=0
   """)


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Model_Id']==df_sink['Model_Id'],"left").select(df_src['Model_Id'],df_src['model_category'],df_sink['dim_model_key'])




# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_model_key").isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new
# MAGIC

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_model_key").isNull()).select(df_src["Model_Id"],df_src["model_category"])
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Fetch the max surrogate key from existing table

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_model_key) from cardatabricks2.gold.dim_model")
    max_value=max_value_df.collect()[0][0]+1



# COMMAND ----------

# MAGIC %md
# MAGIC ### Create surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_model_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create final df - df_filter_old+df_filter_new

# COMMAND ----------

df_final=df_filter_old.union(df_filter_new)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental run
if spark.catalog.tableExists("cardatabricks2.gold.dim_model"):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_model")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_model_key=src.dim_model_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
 
   
#initial run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_model")\
        .saveAsTable("cardatabricks2.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cardatabricks2.gold.dim_model

# COMMAND ----------

