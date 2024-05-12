# Databricks notebook source
# MAGIC %run "./Utilities notebook health"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create database

# COMMAND ----------

# MAGIC %sql
# MAGIC create database healthcare

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use database healthcare

# COMMAND ----------

# MAGIC %md
# MAGIC #Process to silver layer

# COMMAND ----------

health_schema = StructType([
    StructField("STATUS_UPDATE_ID",IntegerType(),False),
    StructField("PATIENT_ID",IntegerType(),False),
    StructField("DATE_PROVIDED",StringType(),False),
    StructField("FEELING_TODAY",StringType(),False),
    StructField("IMPACT",StringType(),False),
    StructField("INJECTION_SITE_SYMPTOMS",StringType(),False),
    StructField("HIGHEST_TEMP",DoubleType(),False),
    StructField("FEVERISH_TODAY",StringType(),False),
    StructField("GENERAL_SYMPTOMS",StringType(),False),
    StructField("HEALTHCARE_VISIT",StringType(),False),
])

# COMMAND ----------

health = spark.read.csv('/mnt/health-updates/bronze/health_status_updates.csv',header=True,schema=health_schema)

# COMMAND ----------

health.head(5)

# COMMAND ----------

silver_health = health.withColumn("DATE_PROVIDED",to_date("DATE_PROVIDED",'MM/dd/yyyy'))

# COMMAND ----------

updated_timestamp(silver_health).write.option('path','/mnt/health-updates/silver/health_data').saveAsTable('health_data',mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended health_data
