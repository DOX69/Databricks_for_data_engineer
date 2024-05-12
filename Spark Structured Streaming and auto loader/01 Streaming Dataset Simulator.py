# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Dataset Simulator
# MAGIC
# MAGIC ##### This notebook is simulating a streaming dataset by incrementally loading a single order at a time

# COMMAND ----------

# Reading the full orders dataset into a DataFrame called orders_full
# Please update with your specific file path and assign it to the variable orders_full_path
orders_full_path = "/mnt/streaming-demo/full_dataset/orders_full.csv"

orders_full = spark.read.csv(orders_full_path, header=True)

# COMMAND ----------

# Displaying the full orders dataset
orders_full.display()

# COMMAND ----------

# You can filter a single order from the full dataset by using the filter method
orders_full.filter(orders_full['ORDER_ID']==1).display()

# COMMAND ----------

# Please copy in your specific file path to the streaming dataset and assign it to the variable orders_streaming_path
orders_streaming_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"

# COMMAND ----------

# Loading the first order into the streaming dataset

order_1 = orders_full.filter(orders_full['ORDER_ID']==1)
order_1.write.options(header=True).mode('append').csv(orders_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# COMMAND ----------

# Loading the second order into the streaming dataset
order_2 = orders_full.filter(orders_full['ORDER_ID']==2)
order_2.write.options(header=True).mode('append').csv(orders_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# COMMAND ----------

# Loading the third order into the streaming dataset
order_3 = orders_full.filter(orders_full['ORDER_ID']==3)
order_3.write.options(header=True).mode('append').csv(orders_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# COMMAND ----------

# Loading the 4th and 5th order into the streaming dataset
orders_4_5 = orders_full.filter( (orders_full['ORDER_ID'] == 4) | (orders_full['ORDER_ID']==5))
orders_4_5.write.options(header=True).mode('append').csv(orders_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# COMMAND ----------

# Loading the 6th and 7th order into the streaming dataset
orders_6_7 = orders_full.filter( (orders_full['ORDER_ID'] == 6) | (orders_full['ORDER_ID']==7))
orders_6_7.write.options(header=True).mode('append').csv(orders_streaming_path)

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv(orders_streaming_path, header=True)
orders_streaming.display()

# COMMAND ----------

# Deleting the streaming dataset
dbutils.fs.rm(orders_streaming_path, recurse=True)

# COMMAND ----------


