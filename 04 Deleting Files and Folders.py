# Databricks notebook source
# MAGIC %fs
# MAGIC rm 'dbfs:/FileStore/tables/countries.txt'

# COMMAND ----------

# MAGIC %fs
# MAGIC rm 'dbfs:/FileStore/tables/countries_single_line.json'

# COMMAND ----------


dbutils.fs.rm('dbfs:/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/output',recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/tables/'

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries_out',recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/tables/'
