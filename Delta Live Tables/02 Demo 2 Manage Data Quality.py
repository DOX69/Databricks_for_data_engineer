# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Orders - Delta Live Tables with Expectations
# MAGIC This Notebook demonstrates how to create a notebook for a Delta Live Table Pipeline with Expectations to manage data quality

# COMMAND ----------

# MAGIC %md
# MAGIC If you have any issues starting your cluster please modify the pipeline settings via JSON and overwrite the cluster config with the snippet below
# MAGIC
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "node_type_id": "Standard_DS3_v2",
# MAGIC             "driver_node_type_id": "Standard_DS3_v2"
# MAGIC         }
# MAGIC     ],

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations
# MAGIC
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-expectations

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Tables

# COMMAND ----------

#ORDERS BRONZE TABLE 

# COMMAND ----------

# Please update the filepath accordingly
orders_path = "/mnt/streaming-demo/full_dataset/orders_full.csv"

orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here I have two expectaions, the first one is called order_status_complete

# COMMAND ----------

@dlt.table
@dlt.expect("ORDER_STATUS_COMPLETE", "ORDER_STATUS == 'COMPLETE'")
@dlt.expect("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def orders_bronze():
    return spark.read.format("csv").option("header",True).schema(orders_schema).load(orders_path)

# COMMAND ----------

# ORDER_ITEMS BRONZE TABLE

# COMMAND ----------

# Please update the filepath accordingly
order_items_path = "/mnt/streaming-demo/full_dataset/order_items_full.csv"

order_items_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("LINE_ITEM_ID", IntegerType(), False),
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False),
                    StructField("QUANTITY", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

@dlt.table
@dlt.expect("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def order_items_bronze():
    return spark.read.format("csv").option("header",True).schema(order_items_schema).load(order_items_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Tables

# COMMAND ----------

# ORDERS SILVER TABLE

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("ORDER_STATUS_COMPLETE", "ORDER_STATUS == 'COMPLETE'")
@dlt.expect_or_drop("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def orders_silver():
    return (
        dlt.read("orders_bronze")
              .select(
              'ORDER_ID', \
              to_date('ORDER_DATETIME', "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_DATE'), \
              'CUSTOMER_ID', \
              'ORDER_STATUS', \
              'STORE_ID',
              current_timestamp().alias("MODIFIED_DATE")
               )
          )

# COMMAND ----------

# ORDER_ITEMS SILVER TABLE

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def order_items_silver():
    return (
        dlt.read("order_items_bronze")
              .select(
              'ORDER_ID', \
              'PRODUCT_ID', \
              'UNIT_PRICE', \
              'QUANTITY',
              current_timestamp().alias("MODIFIED_DATE")
               )
          )
