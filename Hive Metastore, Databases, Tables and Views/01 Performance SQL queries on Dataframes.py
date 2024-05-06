# Databricks notebook source
countries = spark.read.csv('dbfs:/FileStore/tables/countries.csv',header=True)


# COMMAND ----------

# MAGIC %md
# MAGIC # Create temp view

# COMMAND ----------

countries.createOrReplaceTempView('countries')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from countries limit 100

# COMMAND ----------

spark.sql("select * from countries").head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a global temporary view

# COMMAND ----------

countries.createOrReplaceGlobalTempView('countries_gv')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- for a global temp view, you have to specifiy global_temp.<view name>
# MAGIC select * from global_temp.countries_gv limit 1
