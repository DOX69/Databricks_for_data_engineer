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

# MAGIC %md
# MAGIC *deltaTable* : this is a delta table object (not a DF)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***MERGE***

# COMMAND ----------

# MAGIC %md
# MAGIC Let's first retrieve all row of countries and store it in a managed delta table 

# COMMAND ----------

countries_complete = spark.read.csv('/mnt/bronze/countries.csv',header=True,inferSchema=True)

# COMMAND ----------

countries_complete.write.format("delta").saveAsTable('countries_managed_delta_complete')

# COMMAND ----------

# MAGIC %md
# MAGIC Then let's merge with countries_managed_delta
# MAGIC * if matched then update country_code (which is 'YYY' right now for region ID=10)
# MAGIC * if not matched then insert all column

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into countries_managed_delta as t
# MAGIC using countries_managed_delta_complete as u
# MAGIC on t.country_id= u.country_id
# MAGIC when matched 
# MAGIC then update set 
# MAGIC t.COUNTRY_CODE = u.COUNTRY_CODE
# MAGIC when not matched 
# MAGIC then insert (
# MAGIC t.COUNTRY_ID,
# MAGIC t.NAME,
# MAGIC t.NATIONALITY,
# MAGIC t.COUNTRY_CODE,
# MAGIC t.ISO_ALPHA2,
# MAGIC t.CAPITAL,
# MAGIC t.POPULATION,
# MAGIC t.AREA_KM2,
# MAGIC t.REGION_ID,
# MAGIC t.SUB_REGION_ID,
# MAGIC t.INTERMEDIATE_REGION_ID,
# MAGIC t.ORGANIZATION_REGION_ID
# MAGIC )
# MAGIC values (
# MAGIC u.COUNTRY_ID,
# MAGIC u.NAME,
# MAGIC u.NATIONALITY,
# MAGIC u.COUNTRY_CODE,
# MAGIC u.ISO_ALPHA2,
# MAGIC u.CAPITAL,
# MAGIC u.POPULATION,
# MAGIC u.AREA_KM2,
# MAGIC u.REGION_ID,
# MAGIC u.SUB_REGION_ID,
# MAGIC u.INTERMEDIATE_REGION_ID,
# MAGIC u.ORGANIZATION_REGION_ID
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's see if it's work

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from countries_managed_delta
# MAGIC WHERE REGION_ID=20
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC In python 
# MAGIC > Note : the source we use must be a DataFrame

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# COMMAND ----------

countries_managed_delta_complete =spark.read.table('delta_lake_db.countries_managed_delta_complete')

# COMMAND ----------

deltaTable.alias("target").merge(
    countries_managed_delta_complete.alias("source"),
    "target.country_id = source.country_id") \
  .whenMatchedUpdate(set = { "target.country_code" : "source.country_code" } ) \
  .whenNotMatchedInsert(values =
    {
     "country_id": "source.country_id",
      "name": "source.name",
      "nationality": "source.nationality",
      "country_code": "source.country_code",
      "iso_alpha2": "source.iso_alpha2",
      "capital": "source.capital",
      "population": "source.population",
      "area_km2": "source.area_km2",
      "region_id": "source.region_id",
      "sub_region_id": "source.sub_region_id",
      "intermediate_region_id": "source.intermediate_region_id",
      "organization_region_id": "source.organization_region_id"
    }
  ) \
  .execute()
