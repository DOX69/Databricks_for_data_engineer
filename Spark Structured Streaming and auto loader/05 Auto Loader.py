# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader
# MAGIC
# MAGIC #### Resources:
# MAGIC * Auto Loader: https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/

# COMMAND ----------

# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

# For a streaming source DataFrame we must define the schema
# Please update the filepath accordingly
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

# Using Auto Loader to read the streaming data, notice the cloudFiles format that is required for Auto Loader
orders_sdf = spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").schema(orders_schema).load(orders_streaming_path, header=True)

# COMMAND ----------

#Initialising the stream
orders_sdf.display()
