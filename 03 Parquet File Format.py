# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header = 'True', inferSchema= True)

# COMMAND ----------

df.write.parquet('dbfs:/FileStore/tables/output/countries_parquet')

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/tables/output/countries_parquet').display()
