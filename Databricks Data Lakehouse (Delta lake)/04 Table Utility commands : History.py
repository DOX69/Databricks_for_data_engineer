# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.delta.io/0.5.0/delta-utility.html

# COMMAND ----------

# MAGIC %md
# MAGIC #History

# COMMAND ----------

# MAGIC %md
# MAGIC ## In SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta_lake_db.countries_managed_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## In python

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# COMMAND ----------

deltaTable.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Time travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## In SQL
# MAGIC > using : VERSION AS OF 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from delta_lake_db.countries_managed_delta version as of 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## In python
# MAGIC > option("versionAsOf",< version >)

# COMMAND ----------

spark.read.format("delta").option("versionAsOf",1).table('delta_lake_db.countries_managed_delta').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Convert a parquet table to a delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC use database delta_lake_db
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries_managed_parquet

# COMMAND ----------

# Convert unpartitioned parquet table at path '/path/to/table'
deltaTable = DeltaTable.convertToDelta(spark, "parquet.`dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_parquet`")


# COMMAND ----------

# MAGIC %md
# MAGIC See here the history

# COMMAND ----------

deltaTable.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC > But in database, it's still a ***parquet provider***

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended countries_managed_parquet
