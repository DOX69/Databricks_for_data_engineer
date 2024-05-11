# Databricks notebook source
# MAGIC %md
# MAGIC # Order details
# MAGIC
# MAGIC Create an order_details table that contains the following attributes:
# MAGIC
# MAGIC - ORDER ID
# MAGIC - ORDER DATE
# MAGIC - CUSTOMER ID
# MAGIC - STORE NAME
# MAGIC - TOTAL ORDER AMOUNT 
# MAGIC
# MAGIC The table should be aggregated by ORDER ID, ORDER DATE, CUSTOMER ID and STORE NAME to show the TOTAL ORDER AMOUNT.
# MAGIC
# MAGIC Hint: Please consider the order of operations when finding the TOTAL ORDER AMOUNT.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders = spark.read.parquet('dbfs:/FileStore/silver/orders')


# COMMAND ----------

order_items = spark.read.parquet('dbfs:/FileStore/silver/order_items')

# COMMAND ----------

gold_order_items = order_items\
                                .withColumn("ORDER_AMOUNT",col("UNIT_PRICE")*col("QUANTITY"))\
                                    .groupBy("ORDER_ID").agg(round(sum("ORDER_AMOUNT"),2)\
                                                            .alias("TOTAL_ORDER_AMOUNT"))
gold_orders = orders.join(gold_order_items,
                          on = "ORDER_ID",
                          how= "left")\
                    .withColumn("ORDER_DATE",col("ORDER_TIMESTAMP").cast("Date")).drop("ORDER_TIMESTAMP")

# COMMAND ----------

gold_orders.write.parquet("dbfs:/FileStore/gold/order_details",mode= "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # MONTHLY_SALES
# MAGIC
# MAGIC Create an aggregated table to show the monthly sales total and save it in the gold layer as a parquet file called MONTHLY_SALES.
# MAGIC
# MAGIC The table should have two columns:
# MAGIC - MONTH_YEAR - this should be in the format yyyy-MM e.g. 2020-10
# MAGIC - TOTAL_SALES
# MAGIC
# MAGIC Display the sales total rounded to 2 dp and sorted in descending date order
# MAGIC

# COMMAND ----------

gold_monthly_sales = gold_orders.groupBy(date_format(col("ORDER_DATE"),'yyyy-MM').alias("MONTH_YEAR"))\
    .agg(round(sum("TOTAL_ORDER_AMOUNT"),2).alias("TOTAL_SALES",metadata={'Currency':'$ USD'}))\
        .sort(col("MONTH_YEAR").desc())

# COMMAND ----------

gold_monthly_sales.write.parquet("dbfs:/FileStore/gold/monthly_sales",mode= "overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # STORE_MONTHLY_SALES
# MAGIC
# MAGIC Create an aggregated table to show the monthly sales total by store and save it in the gold layer as a parquet file called STORE_MONTHLY_SALES.
# MAGIC
# MAGIC The table should have three columns:
# MAGIC - MONTH_YEAR - this should be in the format yyyy-MM e.g. 2020-10
# MAGIC - STORE NAME
# MAGIC - TOTAL_SALES
# MAGIC
# MAGIC Display the sales total rounded to 2 dp and sorted in descending date order.
# MAGIC

# COMMAND ----------

STORE_MONTHLY_SALES = gold_orders.groupBy(date_format(col("ORDER_DATE"),'yyyy-MM').alias("MONTH_YEAR"),
                    col("STORE_NAME"))\
    .agg(round(sum("TOTAL_ORDER_AMOUNT"),2).alias("TOTAL_SALES",metadata={'Currency':'$ USD'}))\
        .sort(col("MONTH_YEAR").desc())

# COMMAND ----------

STORE_MONTHLY_SALES.write.parquet('dbfs:/FileStore/gold/store_monthly_sales',mode='overwrite')
