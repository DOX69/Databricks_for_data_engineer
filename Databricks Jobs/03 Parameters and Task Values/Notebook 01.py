# Databricks notebook source
name = 'James'

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'name', value = name)

# COMMAND ----------

# Used to retrieve a task input parameter defined on task of Databricks Job
print(dbutils.widgets.get("job_id"))
