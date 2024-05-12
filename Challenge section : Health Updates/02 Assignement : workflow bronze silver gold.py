# Databricks notebook source
# MAGIC %run "./Utilities notebook health"

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
health = spark.read.csv('/mnt/health-updates/bronze/health_status_updates.csv',header=True,schema=health_schema)
silver_health = health.withColumn("DATE_PROVIDED",to_date("DATE_PROVIDED",'MM/dd/yyyy'))

# COMMAND ----------

# updated_timestamp(silver_health).printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC use database healthcare

# COMMAND ----------

deltaTable_health = DeltaTable.forPath(spark,'/mnt/health-updates/silver/health_data')
# deltaTable_health.clone("health_data_clone")

# COMMAND ----------

deltaTable_health.alias('target').merge(
    updated_timestamp(silver_health).alias("source"),
    "target.STATUS_UPDATE_ID = source.STATUS_UPDATE_ID"
)\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

# COMMAND ----------

# %sql
# select * from health_data 
# except 
# select * from health_data_clone

# COMMAND ----------

# MAGIC %md
# MAGIC #Process to gold layer

# COMMAND ----------

health_data = spark.read.table('healthcare.health_data')
# health_data_verif = spark.read.format("delta").load('/mnt/health-updates/silver/health_data')


# COMMAND ----------

# health_data_verif.createOrReplaceTempView("health_data_verif")
# health_data.createOrReplaceTempView("health_data")


# COMMAND ----------

# %sql
# select * from health_data_verif
# except
# select * from health_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### feeling count day

# COMMAND ----------

feeling_count_day = health_data.groupBy("DATE_PROVIDED","FEELING_TODAY").agg(count("PATIENT_ID").alias("TOTAL_PATIENTS"))

# COMMAND ----------

feeling_count_day.write.option('path','/mnt/health-updates/gold/feeling_count_day').saveAsTable("feeling_count_day",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Symptoms count day

# COMMAND ----------

symptoms_count_day = health_data.groupBy("DATE_PROVIDED","GENERAL_SYMPTOMS").agg(count("PATIENT_ID").alias("TOTAL_PATIENTS"))

# COMMAND ----------

symptoms_count_day.write.option('path','/mnt/health-updates/gold/symptoms_count_day').saveAsTable("symptoms_count_day",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Healthcare visit day

# COMMAND ----------

healthcase_visit_day = health_data.groupBy("DATE_PROVIDED","HEALTHCARE_VISIT").agg(count("PATIENT_ID").alias("TOTAL_PATIENTS"))
healthcase_visit_day.write.option('path','/mnt/health-updates/gold/healthcase_visit_day').saveAsTable("healthcase_visit_day",mode="overwrite")
