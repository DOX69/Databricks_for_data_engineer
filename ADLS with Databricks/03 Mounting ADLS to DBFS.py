# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

application_id = "a54c2834-3bff-4943-8cfd-bbebaee25310"
tenant_id = "ef45c45c-4142-434c-b13e-f7ac7c104097"
secret= "2sj8Q~ADMvVBXNh9cns24qVy7XOFqZikRS~PocCX"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalake202405.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

spark.read.csv('/mnt/bronze/country_regions.csv',header=True).head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #To unmount

# COMMAND ----------

# dbutils.fs.unmount('/mnt/bronze')
