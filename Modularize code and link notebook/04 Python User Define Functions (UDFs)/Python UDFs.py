# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC > use length() which is a pyspark function

# COMMAND ----------

def count_chars(col):
    return length(col)

# COMMAND ----------

# MAGIC %md
# MAGIC > len() which is a pure python function

# COMMAND ----------

def count_chars_py(col):
    return len(col)

# COMMAND ----------

df = spark.read.csv('/FileStore/bronze/customers.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC > Let's try length()

# COMMAND ----------

df.withColumn("Length_full_name",count_chars("FULL_NAME")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > But if we use the python function len()
# MAGIC
# MAGIC > We got an error because len() is not a native function in spark
# MAGIC
# MAGIC > We have to use UDF function
# MAGIC
# MAGIC >https://learn.microsoft.com/en-us/azure/databricks/udf/
# MAGIC
# MAGIC > Thanks to that, we can define complexe function in other language like scala, pandas or R

# COMMAND ----------

df.withColumn("Length_full_name",count_chars_py("FULL_NAME")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > let's declare it

# COMMAND ----------

count_chars_py = udf(count_chars_py)

# COMMAND ----------

df.withColumn("Length_full_name",count_chars_py("FULL_NAME")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > Or define directly create the function using **lambda col : len(col)**

# COMMAND ----------

count_chars_py = udf(
    lambda col:len(col)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Limitations

# COMMAND ----------

# MAGIC %md
# MAGIC > Other languages is less efficent and slower

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's test this by adding more data to *customers* table

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC >> Add more rows to our dataFrame with a loop

# COMMAND ----------

new_df = df
for _ in range(100):
    df=df.union(new_df)
# df.count()

# COMMAND ----------

df.withColumn("Length_full_name",count_chars("FULL_NAME"))
## run time : 0.11s

# COMMAND ----------


df.withColumn("Length_full_name",count_chars_py("FULL_NAME"))
## run time : 0.21s

# COMMAND ----------

# MAGIC %md
# MAGIC We almost double the runtime
