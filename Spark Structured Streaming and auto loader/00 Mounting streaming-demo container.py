# Databricks notebook source
# MAGIC %md
# MAGIC ###Secrets

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key='tenant-id')
application_id = dbutils.secrets.get(scope='dbx-secrets-202405',key='application-id')
secret = dbutils.secrets.get(scope='dbx-secrets-202405',key='secret')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Config
# MAGIC

# COMMAND ----------

container_name = "streaming-demo"
account_name ="datalake202405"
mount_point = "/mnt/streaming-demo"

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
