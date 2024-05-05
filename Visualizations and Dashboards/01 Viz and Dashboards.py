# Databricks notebook source
order_details = spark.read.parquet("/FileStore/gold/order_details")
monthly_sales = spark.read.parquet("/FileStore/gold/monthly_sales")

# COMMAND ----------

display(order_details)

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC Some text for my Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC #Title for my dashboard

# COMMAND ----------

# You can add HTML titles and text to your dashboard
displayHTML("""<font size="6" color="red" face="sans-serif">Sales Dashboard</font>""")
