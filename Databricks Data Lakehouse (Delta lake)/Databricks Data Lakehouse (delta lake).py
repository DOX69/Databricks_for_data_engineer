# Databricks notebook source
tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key="tenant-id")
application_id= dbutils.secrets.get(scope='dbx-secrets-202405',key="application-id")
secret = dbutils.secrets.get(scope='dbx-secrets-202405',key="secret")

# COMMAND ----------

container_name = "delta-lake-demo"
account_name ="datalake202405"
mount_point = "/mnt/delta-lake-demo"

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

display(dbutils.fs.ls('/mnt/'))

# COMMAND ----------

countries = spark.read.csv('/mnt/bronze/countries.csv',header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the countries in delta format

# COMMAND ----------

countries.write.format("delta").save(f"{mount_point}/countries_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC partition by

# COMMAND ----------

countries.write.format("delta").partitionBy("REGION_ID").save(f"{mount_point}/countries_delta_part",mode='overwrite')

# COMMAND ----------

countries.write.format("parquet").save(f"{mount_point}/countries_parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the countries delta/parquet

# COMMAND ----------

spark.read.format("delta").load(f"{mount_point}/countries_delta").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create the delta table in a database : a managed table delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC create database delta_lake_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC use database delta_lake_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

countries = spark.read.format("delta").load(f"{mount_point}/countries_delta")

# COMMAND ----------

countries.write.saveAsTable('countries_managed_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries_managed_delta

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###Create the delta table in a database : a external table delta format

# COMMAND ----------

countries.write.option('path',f"{mount_point}/countries_delta").saveAsTable('countries_ext_delta',mode='overwrite')
