# Databricks notebook source
# MAGIC %md
# MAGIC #Customer Orders - Delta Live Tables
# MAGIC

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### how to create DLT
# MAGIC     ```@dlt.table
# MAGIC     def <function-name()>: 
# MAGIC         return <dataFrame query>```
# MAGIC > the function name will also be the name of the delta live table

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
# MAGIC ###Orders Broner table

# COMMAND ----------

# For a streaming source DataFrame we must define the schema
# Please update the filepath accordingly
orders_streaming_path = "/mnt/streaming-demo/full_dataset/orders_full.csv"
 
orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )


# COMMAND ----------

@dlt.table
def orders_bronze() :
    df = spark.read.format("csv").option("header",True).schema(orders_schema).load(orders_streaming_path)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order items bronze

# COMMAND ----------

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
def order_items_bronze():
    return spark.read.option("header",True).schema(order_items_schema).csv(order_items_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders silver

# COMMAND ----------

@dlt.table
def orders_silver():
    df_orders_silver = dlt.read("orders_bronze").\
        withColumns({"ORDER_DATE":to_date("ORDER_DATETIME","dd-MMM-yy hh.mm.ss.SS"),
                     "MODIFIED_DATE":current_timestamp()}).\
        drop("ORDER_DATETIME")
    return df_orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order_items_silver

# COMMAND ----------

@dlt.table
def Order_items_silver():
  return dlt.read("order_items_bronze").\
    withColumn("MODIFIED_DATE",current_timestamp()).\
      drop("LINE_ITEM_ID")
