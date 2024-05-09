# Databricks notebook source
# MAGIC %md
# MAGIC # Import librairies

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/bronze/customers.csv',header=True,inferSchema=True)
# df1.display()
df2 = spark.read.csv('dbfs:/FileStore/bronze/order_items.csv',header=True,inferSchema=True)
# df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Define functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add a new col

# COMMAND ----------

def add_ingestion_date(df):
    return df.withColumn("INGESTION_TIMESTAMP",current_timestamp())

# COMMAND ----------

# add_ingestion_date(df1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a multiply cols

# COMMAND ----------

def multiply_cols(df,a,b):
    return df.withColumn("AMOUNT",round(a*b,2))

# COMMAND ----------

# multiply_cols(df2,df2["UNIT_PRICE"],df2["QUANTITY"]).display()
