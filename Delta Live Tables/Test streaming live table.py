# Databricks notebook source
# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/order_items_bronze`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/orders_bronze`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/order_items_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/orders_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/order_details_gold`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/streaming-demo/stream_batch_live_tables/tables/monthly_sales_gold`
