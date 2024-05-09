# Databricks notebook source
# MAGIC %run "./Utility notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC #How to use function
# MAGIC > Register it using spark.udf.register({name},{function name},{retunr type})
# MAGIC
# MAGIC >https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.register.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important note :
# MAGIC We don't have to import UDF function because it's already imported from *Utility notebook* (from **from pyspark.sql.functions import***)

# COMMAND ----------

# spark.udf.register("multiply_cols",multiply_cols)

# COMMAND ----------

df1 = spark.read.csv('/FileStore/bronze/order_items.csv', header=True, inferSchema=True)

# COMMAND ----------

df1.createOrReplaceTempView("Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC *, multiply_cols(unit_price,quantity)
# MAGIC from temp limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC #To improve this
# MAGIC > We can directly register the funcion in the utility notebook
