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
# MAGIC select distinct(Dealer_ID)as Dealer_ID ,DealerName from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales`

# COMMAND ----------

df_src=spark.sql("""
   select Distinct(Dealer_Id) as Dealer_Id, DealerName from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   """)
df_src.display()
   

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model Sink-Initial and Incremental(Just Bring Schema if table not Exists
# MAGIC )

# COMMAND ----------

if spark.catalog.tableExists("cardatabricks2.gold.dim_dealer"):
    df_sink=spark.sql("""
   select  dim_dealer_key,Dealer_Id, DealerName from cardatabricks2.gold.dim_dealer
  
   """)


else:
    df_sink=spark.sql("""
   select 1 as dim_dealer_key,Dealer_Id, DealerName from parquet.`abfss://silver@carakanshdatalake.dfs.core.windows.net/car_sales` 
   where 1=0
   """)


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Dealer_Id']==df_sink['Dealer_Id'],"left").select(df_src['Dealer_Id'],df_src['DealerName'],df_sink['dim_dealer_key'])
df_filter.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_dealer_key").isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new
# MAGIC

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_dealer_key").isNull()).select(df_src["Dealer_Id"],df_src["DealerName"])
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
    max_value_df=spark.sql("select max(dim_dealer_key) from cardatabricks2.gold.dim_dealer")
    max_value=max_value_df.collect()[0][0]+1



# COMMAND ----------

# MAGIC %md
# MAGIC ### Create surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_dealer_key",max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists("cardatabricks2.gold.dim_dealer"):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_dealer")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_dealer_key=src.dim_dealer_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
 
   
#initial run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://gold@carakanshdatalake.dfs.core.windows.net/dim_dealer")\
        .saveAsTable("cardatabricks2.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cardatabricks2.gold.dim_dealer

# COMMAND ----------

