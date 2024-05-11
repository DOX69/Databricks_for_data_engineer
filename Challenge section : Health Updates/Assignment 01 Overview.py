# Databricks notebook source
# MAGIC %md
# MAGIC # Mount  the health-Updates container to my DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ###Secrets

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key='tenant-id')
application_id = dbutils.secrets.get(scope='dbx-secrets-202405',key='application-id')
secret = dbutils.secrets.get(scope='dbx-secrets-202405',key='secret')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Config

# COMMAND ----------

container_name = "health-updates"
account_name ="datalake202405"
mount_point = "/mnt/health-updates"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

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

silver_health.display()
