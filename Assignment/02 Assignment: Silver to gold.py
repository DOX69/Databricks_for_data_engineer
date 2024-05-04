# Databricks notebook source
# MAGIC %md
# MAGIC # Order details
# MAGIC
# MAGIC Create an order_details table that contains the following attributes:
# MAGIC
# MAGIC - ORDER ID
# MAGIC - ORDER DATE
# MAGIC - CUSTOMER ID
# MAGIC - STORE NAME
# MAGIC - TOTAL ORDER AMOUNT 
# MAGIC
# MAGIC The table should be aggregated by ORDER ID, ORDER DATE, CUSTOMER ID and STORE NAME to show the TOTAL ORDER AMOUNT.
# MAGIC
# MAGIC Hint: Please consider the order of operations when finding the TOTAL ORDER AMOUNT.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders = spark.read.parquet('dbfs:/FileStore/silver/orders')


# COMMAND ----------

order_items = spark.read.parquet('dbfs:/FileStore/silver/order_items')

# COMMAND ----------

silver_order_items = order_items\
                                .withColumn("ORDER_AMOUNT",col("UNIT_PRICE")*col("QUANTITY"))\
                                    .groupBy("ORDER_ID").agg(round(sum("ORDER_AMOUNT"),2)\
                                                            .alias("TOTAL_ORDER_AMOUNT"))
