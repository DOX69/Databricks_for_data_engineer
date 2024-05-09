# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC # Run an other notebook

# COMMAND ----------

dbutils.notebook.run('Worker notebook',60)

# COMMAND ----------

# MAGIC %md
# MAGIC #IF worker notebook fails : run a message

# COMMAND ----------

try :
    dbutils.notebook.run('Worker notebook',60)
except:
    print('error occured')

# COMMAND ----------

print('next command')

# COMMAND ----------

# MAGIC %md
# MAGIC # Text widgets

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

# MAGIC %md
# MAGIC > Widget text are paramaeter that can be passed to an other notebook

# COMMAND ----------

dbutils.notebook.run("Worker notebook",60,{'Text_input':'Run from master notebook'})
