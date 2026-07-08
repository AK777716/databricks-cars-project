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

df_src=spark.sql("""
   select Distinct(Date_Id) as Date_Id from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   """)
df_src.display()
   

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model Sink-Initial and Incremental(Just Bring Schema if table not Exists
# MAGIC )

# COMMAND ----------

if spark.catalog.tableExists("cardatabricks2.gold.dim_date"):
    df_sink=spark.sql("""
   select  dim_date_key,Date_Id from cardatabricks2.gold.dim_date
  
   """)


else:
    df_sink=spark.sql("""
   select 1 as dim_date_key,Date_Id from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   where 1=0
   """)


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Date_Id']==df_sink['Date_Id'],"left").select(df_src['Date_Id'],df_sink['dim_date_key'])
df_filter.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_date_key").isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new
# MAGIC

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_date_key").isNull()).select(df_src["Date_Id"])
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
    max_value_df=spark.sql("select max(dim_date_key) from cardatabricks2.gold.dim_date")
    max_value=max_value_df.collect()[0][0]+1



# COMMAND ----------

# MAGIC %md
# MAGIC ### Create surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_date_key",max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists("cardatabricks2.gold.dim_date"):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_date")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_date_key=src.dim_date_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
 
   
#initial run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_date")\
        .saveAsTable("cardatabricks2.gold.dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cardatabricks2.gold.dim_date

# COMMAND ----------

