# Databricks notebook source
print("Worker for master notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC #Test widget text

# COMMAND ----------

dbutils.widgets.text("Text_input",'',label="Add text")

# COMMAND ----------

message = print(f"Message is :",dbutils.widgets.get('Text_input'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook exit command
# MAGIC > You can use this to end the notebook and print a message
# MAGIC
# MAGIC *Note*: you have to put this at the end of the notebook, otherwise the other code will note be run

# COMMAND ----------

# dbutils.notebook.exit('Worker notebook Executed Successfully')
