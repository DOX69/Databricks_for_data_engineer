# Databricks notebook source
# MAGIC %md
# MAGIC # Reading a Data Stream
# MAGIC
# MAGIC #### Resources:
# MAGIC * Structured Streaming API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html
# MAGIC * DataStreamReader API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html
# MAGIC * DataStreamWriter API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html
# MAGIC * Input / Output: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/io.html
# MAGIC * Query Management: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/query_management.html

# COMMAND ----------

# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# For a streaming source DataFrame we must define the schema
# Please update with your specific file path and assign it to the variable orders_full_path
orders_streaming_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
 
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
# MAGIC ## Reading the streaming data into a dataframe
# MAGIC using **park.readStream.**
# MAGIC >The 'S' in upper case is important

# COMMAND ----------

orders_sdf = spark.readStream.csv(orders_streaming_path, schema=orders_schema, header=True )

# COMMAND ----------

# MAGIC %md
# MAGIC Let's display the data
# MAGIC >> This will be infinitly running and show some stats
# MAGIC
# MAGIC >> **Don't forget to cancel them manually** !!

# COMMAND ----------

orders_sdf.display()
