# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Dataset Simulator
# MAGIC
# MAGIC ##### This notebook is simulating a streaming dataset by incrementally loading a single order at a time

# COMMAND ----------

orders_full_path = "/mnt/streaming-demo/full_dataset/orders_full.csv"

order_items_full_path = "/mnt/streaming-demo/full_dataset/order_items_full.csv"

# COMMAND ----------

# Reading the ORDERS_FULL dataset into a DataFrame called orders_full
orders_full = spark.read.csv(orders_full_path, header=True)

# Reading the ORDER_ITEMS_FULL dataset into a DataFrame called orders_full
order_items_full = spark.read.csv(order_items_full_path, header=True)

# COMMAND ----------

orders_streaming_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"

order_items_streaming_path = "/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"

# COMMAND ----------

# Loading the first order into the ORDERS_STREAMING dataset
order_1 = orders_full.filter(orders_full['ORDER_ID']==1)
order_1.write.options(header=True).mode('append').csv(orders_streaming_path)

# Loading the first order into the ORDER_ITEMS_STREAMING dataset
order_item_1 = order_items_full.filter(order_items_full['ORDER_ID']==1)
order_item_1.write.options(header=True).mode('append').csv(order_items_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# Reading the order_items_streaming dataset via a DataFrame
order_items_streaming = spark.read.csv(order_items_streaming_path, header=True)
order_items_streaming.display()

# COMMAND ----------

# Loading the second order into the ORDERS_STREAMING dataset
order_2 = orders_full.filter(orders_full['ORDER_ID']==2)
order_2.write.options(header=True).mode('append').csv(orders_streaming_path)

# Loading the second order into the ORDER_ITEMS_STREAMING dataset
order_items_2 = order_items_full.filter(order_items_full['ORDER_ID']==2)
order_items_2.write.options(header=True).mode('append').csv(order_items_streaming_path)

# COMMAND ----------

# Loading the third order into the ORDERS_STREAMING dataset
order_3 = orders_full.filter(orders_full['ORDER_ID']==3)
order_3.write.options(header=True).mode('append').csv(orders_streaming_path)

# Loading the third order into the ORDER_ITEMS_STREAMING dataset
order_items_3 = order_items_full.filter(order_items_full['ORDER_ID']==3)
order_items_3.write.options(header=True).mode('append').csv(order_items_streaming_path)

# COMMAND ----------

# Loading the 4th and 5th order into the ORDERS_STREAMING dataset
orders_4_5 = orders_full.filter( (orders_full['ORDER_ID'] == 4) | (orders_full['ORDER_ID']==5))
orders_4_5.write.options(header=True).mode('append').csv(orders_streaming_path)

# Loading the 4th and 5th order into the ORDER_ITEMS_STREAMING dataset
order_items_4_5 = order_items_full.filter( (order_items_full['ORDER_ID'] == 4) | (order_items_full['ORDER_ID']==5))
order_items_4_5.write.options(header=True).mode('append').csv(order_items_streaming_path)

# COMMAND ----------

# Deleting the streaming datasets
dbutils.fs.rm(orders_streaming_path, recurse=True)
dbutils.fs.rm(order_items_streaming_path, recurse=True)

# COMMAND ----------


