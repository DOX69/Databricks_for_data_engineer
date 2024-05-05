# Databricks notebook source
rnGsL9dYt+JgYcFaiSowcHnrHp9+jdqTvYv9i7xzZDoINXTCcG5whkN5Tf8MEq7Vn46FZQgE3tlP+AStqKBubw==

# COMMAND ----------

# Paste in your account key in the second argument
spark.conf.set(
    "fs.azure.account.key.datalake202405.dfs.core.windows.net",
    "rnGsL9dYt+JgYcFaiSowcHnrHp9+jdqTvYv9i7xzZDoINXTCcG5whkN5Tf8MEq7Vn46FZQgE3tlP+AStqKBubw==")

# COMMAND ----------

countries = spark.read.csv('abfss://bronze@datalake202405.dfs.core.windows.net/countries.csv',header=True)

# COMMAND ----------

country_regions = spark.read.csv('abfss://bronze@datalake202405.dfs.core.windows.net/country_regions.csv',header=True)
