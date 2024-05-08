# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS to DBFS
# MAGIC
# MAGIC #### Resources:
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

application_id = "a54c2834-3bff-4943-8cfd-bbebaee25310"
tenant_id = "ef45c45c-4142-434c-b13e-f7ac7c104097"
secret= "2sj8Q~ADMvVBXNh9cns24qVy7XOFqZikRS~PocCX"

# COMMAND ----------

container_name = "bronze"
account_name ="datalake202405"
mount_point = "/mnt/bronze"

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

dbutils.fs.unmount(mount_point)

# COMMAND ----------

# MAGIC %md
# MAGIC # Access to secret

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key="tenant-id")

# COMMAND ----------

application_id= dbutils.secrets.get(scope='dbx-secrets-202405',key="application-id")

# COMMAND ----------

secret = dbutils.secrets.get(scope='dbx-secrets-202405',key="secret")

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
