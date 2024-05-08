# Databricks notebook source
# MAGIC %md
# MAGIC * https://docs.delta.io/0.5.0/delta-update.html

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_delta

# COMMAND ----------

countries = spark.read.csv("/mnt/bronze/countries.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a parquet managed table

# COMMAND ----------

countries.write.format("parquet").saveAsTable("delta_lake_db.countries_managed_parquet",mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC use database delta_lake_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC * 
# MAGIC from countries_managed_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manipulate table with some DML ( Data Management Languages )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***DELETE***

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC the following code will display an error because countries_managed_parquet is **not a delta table**

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM countries_managed_parquet
# MAGIC WHERE REGION_ID=20

# COMMAND ----------

# MAGIC %md
# MAGIC The following DELETE work because countries_managed_delta **is a delta table**

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM countries_managed_delta
# MAGIC WHERE REGION_ID=20

# COMMAND ----------

# MAGIC %md
# MAGIC Performe a DELETE in Python for delta table

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# COMMAND ----------

deltaTable.delete("REGION_ID =50")


# COMMAND ----------

deltaTable.delete((col("region_id")==40)&(col('population')>200000))   

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***UPDATE***

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_lake_db.countries_managed_delta
# MAGIC set COUNTRY_CODE='XXX'
# MAGIC where region_id=10

# COMMAND ----------

# MAGIC %md
# MAGIC Update in python

# COMMAND ----------

deltaTable.update("region_id ==10", { "COUNTRY_CODE": "'ZZZ'" } )   # predicate using SQL formatted string


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from countries_managed_delta

# COMMAND ----------

deltaTable.update(col("region_id") == 10, { "COUNTRY_CODE": lit("YYY") } )   # predicate using Spark SQL function

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from countries_managed_delta
# MAGIC WHERE REGION_ID=10
# MAGIC limit 10
