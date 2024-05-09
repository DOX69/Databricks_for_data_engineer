# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def add_ingestion_date(df):
    return df.withColumn('INGESTION_DATE', current_date())

# COMMAND ----------

def multiply_cols(a,b):
    return a*b

# COMMAND ----------

# MAGIC %md
# MAGIC #Register the function for SQL

# COMMAND ----------

spark.udf.register("multiply_cols",multiply_cols)
