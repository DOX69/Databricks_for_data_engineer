# Databricks notebook source
# MAGIC %md 
# MAGIC ##Libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### updated_timestamp

# COMMAND ----------

def updated_timestamp(df) :
    return df.withColumn("UPDATED_TIMESTAMP",current_timestamp())
