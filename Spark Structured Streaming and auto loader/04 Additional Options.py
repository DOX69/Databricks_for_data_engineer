# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Structured Streaming: Additional Options
# MAGIC
# MAGIC #### Resources:
# MAGIC * Structured Streaming API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html
# MAGIC * DataStreamReader API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html
# MAGIC * DataStreamWriter API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html
# MAGIC * Input / Output: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/io.html
# MAGIC * Query Management: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/query_management.html

# COMMAND ----------

# output mode append, this is set by default
# can also be "complete" or "update"
orders_sdf.writeStream.format("delta").\
outputMode("append").\ 
option("checkpointLocation", checkpoint_loc).\
start(sink_path)

# COMMAND ----------

# specify column to partition the data
orders_sdf.writeStream.format("delta").\
outputMode("append").\
option("checkpointLocation", checkpoint_loc).\
partitionBy("column").\ 
start(sink_path)

# COMMAND ----------

# processing time 5 seconds trigger
orders_sdf.writeStream.format("delta").trigger(processingTime='5 seconds').\
option("checkpointLocation", checkpoint_loc).start(sink_path)

# COMMAND ----------

# run only once trigger
orders_sdf.writeStream.format("delta").\
trigger(once=True).\
option("checkpointLocation", checkpoint_loc).\
start(sink_path)
