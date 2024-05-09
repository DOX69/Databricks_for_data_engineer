# Databricks notebook source
# MAGIC %md
# MAGIC #How to reuse function from an other notebook

# COMMAND ----------

# MAGIC %md
# MAGIC > Use %run **"path/of/notebook"**
# MAGIC >>Must be in double quote " "
# MAGIC
# MAGIC >>Make sure you remove all the display,analysis cells in the notebook
# MAGIC
# MAGIC >>Directly access to the function and outputs and import librairies
# MAGIC
# MAGIC >>if the notebook is in the same folder, use **./** or **../Folder above/Notebook**
# MAGIC
# MAGIC >> Use this **At the start** of the notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mickael.rakotoa@gmail.com/Data engineer/Practice/Git/Databricks_for_data_engineer/Modularize code and link notebook/02 Define function to store reusable logic/Utility notebook"

# COMMAND ----------

# MAGIC %run "./Utility notebook"

# COMMAND ----------

# MAGIC %run "../02 Define function to store reusable logic/Utility notebook"

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/bronze/customers.csv',header=True,inferSchema=True)
df2 = spark.read.csv('dbfs:/FileStore/bronze/order_items.csv',header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC >> So now we can reuse librairies and functions

# COMMAND ----------

multiply_cols(df2,col("UNIT_PRICE"),col("QUANTITY")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Note the same as dbutils.notebook.run()
# MAGIC >dbutils.notebook.run() run the notebook separatly and can't reuse function nor librairies
# MAGIC
# MAGIC >It's like run 2 notebooks that is not related each other
# MAGIC
# MAGIC >https://docs.databricks.com/en/notebooks/notebook-workflows.html
