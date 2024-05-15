# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Orders - Additional Properties and Views
# MAGIC This Notebook demonstrates how to create a notebook for a Delta Live Table Pipelines
# MAGIC
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-python-ref
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-concepts

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
orders_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"

orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

@dlt.table(comment="This is the ORDERS bronze table")
def orders_bronze():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header",True).schema(orders_schema).load(orders_path)

# COMMAND ----------

# ORDER_ITEMS BRONZE TABLE

# COMMAND ----------

# Please update the filepath accordingly
order_items_path = "/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"

order_items_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("LINE_ITEM_ID", IntegerType(), False),
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False),
                    StructField("QUANTITY", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

@dlt.table(comment="This is the ORDER_ITEMS bronze table")
def order_items_bronze():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header",True).schema(order_items_schema).load(order_items_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Tables

# COMMAND ----------

# ORDERS SILVER TABLE

# COMMAND ----------

@dlt.table(comment="This is the ORDERS silver table", partition_cols = ["ORDER_DATE"])
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
              .select(
              'ORDER_ID', \
              to_date('ORDER_DATETIME', "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_DATE'), \
              'CUSTOMER_ID', \
              'ORDER_STATUS', \
              'STORE_ID'
               )
          )

# COMMAND ----------

# ORDER_ITEMS SILVER TABLE

# COMMAND ----------

@dlt.table(comment="This is the ORDER_ITEMS silver table", partition_cols = ["PRODUCT_ID"])
def order_items_silver():
    return (
        dlt.read_stream("order_items_bronze")
              .select(
              'ORDER_ID', \
              'PRODUCT_ID', \
              'UNIT_PRICE', \
              'QUANTITY'
               )
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Tables

# COMMAND ----------

#ORDER_DETAILS GOLD TABLE

# COMMAND ----------

@dlt.view
def order_details_gold():
    # Step 1: Assign the silver layer datasets into variables
    orders_silver = dlt.read("orders_silver")
    order_items_silver = dlt.read("order_items_silver")
    # Step 2: Join the silver datasets and perform data transformations  
    order_details = orders_silver.join(order_items_silver, orders_silver['order_id']==order_items_silver['order_id'], 'left')
    order_details = order_details.select(orders_silver["ORDER_ID"], orders_silver['ORDER_DATE'],orders_silver["CUSTOMER_ID"], order_items_silver["PRODUCT_ID"], order_items_silver["UNIT_PRICE"], order_items_silver["QUANTITY"])
    order_details = order_details.withColumn('SUB_TOTAL_AMOUNT', order_details['UNIT_PRICE']*order_details['QUANTITY'])
    return order_details

# COMMAND ----------

#MONTHLY_SALES GOLD TABLE

# COMMAND ----------

@dlt.table
def monthly_sales_gold():
    
    # Step 1: Assign the ORDER_DETAILS Table to a variable
    order_details = dlt.read("order_details_gold")
    
    # Step 2: Perform data transformations  
    monthly_sales = order_details.withColumn('MONTH_YEAR', date_format(order_details['ORDER_DATE'],'yyyy-MM'))
    monthly_sales = monthly_sales.groupBy('MONTH_YEAR').sum('SUB_TOTAL_AMOUNT')
    monthly_sales = monthly_sales.withColumn('TOTAL_SALES', round('sum(SUB_TOTAL_AMOUNT)',2)).sort(monthly_sales['MONTH_YEAR'].desc())
    monthly_sales = monthly_sales.select('MONTH_YEAR', 'TOTAL_SALES')
    
    return monthly_sales
