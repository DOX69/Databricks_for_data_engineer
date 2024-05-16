# Databricks notebook source
# MAGIC %md
# MAGIC > This method can retrieve a parameter from another notebook in a prior task, the task must be called "notebook_01" and the key must be "name"

# COMMAND ----------

name  = dbutils.jobs.taskValues.get(taskKey = "notebook_01", key = "name", debugValue = 0)

# COMMAND ----------

print(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limitation
# MAGIC You cannot pass a large dataframe or something big. It's limited to a small value around 100KB
