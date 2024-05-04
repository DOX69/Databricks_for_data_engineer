# Databricks notebook source
# MAGIC %md
# MAGIC # Schemas and import data

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructField,StructType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

customers_schema = StructType([
    StructField('CUSTOMER_ID',IntegerType(),False),
    StructField('FULL_NAME',StringType(),False),
    StructField('EMAIL_ADDRESS',StringType(),False),
])

# COMMAND ----------

customers = spark.read.csv('dbfs:/FileStore/bronze/customers.csv',header= True,schema=customers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order_items

# COMMAND ----------

order_items_schema = StructType([
    StructField('ORDER_ID',IntegerType(),False),
    StructField('LINE_ITEM_ID',IntegerType(),False),
    StructField('PRODUCT_ID',IntegerType(),False),
    StructField('UNIT_PRICE',DoubleType(),False),
    StructField('QUANTITY',IntegerType(),False),
])

# COMMAND ----------

orders_items = spark.read.csv('dbfs:/FileStore/bronze/order_items.csv',header=True,schema=order_items_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

orders_schema = StructType([
    StructField('ORDER_ID',IntegerType(),False),
    StructField('ORDER_DATETIME',StringType(),False),
    StructField('CUSTOMER_ID',IntegerType(),False),
    StructField('ORDER_STATUS',StringType(),False),
    StructField('STORE_ID',IntegerType(),False),

        
])

# COMMAND ----------

orders = spark.read.csv('dbfs:/FileStore/bronze/orders.csv',header=True,schema= orders_schema)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Stores

# COMMAND ----------

stores_schema = StructType([
    StructField('STORE_ID',IntegerType(),False),
    StructField('STORE_NAME',StringType(),False),
    StructField('WEB_ADDRESS',StringType(),False),
    StructField('LATITUDE',DoubleType(),False),
    StructField('LONGITUDE',DoubleType(),False),
        
])

# COMMAND ----------

stores = spark.read.csv('dbfs:/FileStore/bronze/stores.csv',header=True,schema=stores_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

products_schema = StructType([
    StructField('PRODUCT_ID',IntegerType(),False),
    StructField('PRODUCT_NAME',StringType(),False),
    StructField('UNIT_PRICE',DoubleType(),False),
])

# COMMAND ----------

products = spark.read.csv('dbfs:/FileStore/bronze/products.csv',header=True,schema=products_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders
# MAGIC Using Orders and stores
# MAGIC
# MAGIC And filter on order status= complete

# COMMAND ----------

orders = orders.filter(orders['ORDER_STATUS']=='COMPLETE')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join and select fields

# COMMAND ----------

silver_orders = orders.join(stores,
                            on = "STORE_ID",
                            how = 'left')\
                       .select("ORDER_ID","ORDER_DATETIME","CUSTOMER_ID","STORE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change order datetime format to timestamp

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

silver_orders = silver_orders.withColumn("ORDER_TIMESTAMP",to_timestamp(silver_orders['ORDER_DATETIME'],'dd-MMM-yy HH.mm.ss.SS')).drop("ORDER_DATETIME")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop columns

# COMMAND ----------

silver_orders_items = orders_items.drop("LINE_ITEM_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete null values

# COMMAND ----------

silver_orders_items = silver_orders_items.na.drop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop null values

# COMMAND ----------

silver_customers = customers.na.drop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop null values

# COMMAND ----------

silver_products = products.na.drop()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write parquet files
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a silver folder

# COMMAND ----------

dbutils.fs.mkdirs('FileStore/silver') ## Not necessary, DBX will create it when write a file  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write down

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

silver_orders.write.mode("overwrite").parquet('FileStore/silver/orders')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders items

# COMMAND ----------

silver_orders_items.write.mode("overwrite").parquet('FileStore/silver/order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

silver_customers.write.mode("overwrite").parquet('FileStore/silver/customers')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

silver_products.write.mode("overwrite").parquet('FileStore/silver/products')
