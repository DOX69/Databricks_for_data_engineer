# Databricks notebook source
# MAGIC %md
# MAGIC # Writing to a Data Stream
# MAGIC
# MAGIC #### Resources:
# MAGIC * Structured Streaming API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html
# MAGIC * DataStreamReader API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html
# MAGIC * DataStreamWriter API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html
# MAGIC * Input / Output: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/io.html

# COMMAND ----------

# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# For a streaming source DataFrame we must define the schema
# Please insert your file path
orders_streaming_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
 
orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )
# reading the streaming data into a dataframe called orders_sdf
orders_sdf = spark.readStream.csv(orders_streaming_path, orders_schema, header=True )

# COMMAND ----------

streamQuery = orders_sdf.writeStream.format("delta").\
    option("checkpointLocation","/mnt/streaming-demo/streaming_dataset/order_stream_sink/_checkpointLocation").\
        start("/mnt/streaming-demo/streaming_dataset/order_stream_sink")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Query Management: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/query_management.html

# COMMAND ----------

streamQuery.isActive

# COMMAND ----------

streamQuery.recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC At this point, we have only 2 records because in the previous lecture we insert only 2 rows in orders_streaming.csv

# COMMAND ----------

spark.read.format("delta").load("/mnt/streaming-demo/streaming_dataset/order_stream_sink").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's create a managed delta streaming table

# COMMAND ----------

# MAGIC %sql
# MAGIC create database streaming_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use database streaming_db

# COMMAND ----------

streamQuery = orders_sdf.writeStream.format("delta").\
  option("checkpointLocation",'/mnt/streaming-demo/streaming_dataset/streaming_db/managed/_checkpointLocation').\
    toTable("orders_managed_delta_stream")
