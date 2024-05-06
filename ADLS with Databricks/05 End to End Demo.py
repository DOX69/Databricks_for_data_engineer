# Databricks notebook source
# MAGIC %md
# MAGIC # Mount the silver layer

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key="tenant-id")
application_id= dbutils.secrets.get(scope='dbx-secrets-202405',key="application-id")
secret = dbutils.secrets.get(scope='dbx-secrets-202405',key="secret")

# COMMAND ----------

container_name = "silver"
account_name ="datalake202405"
mount_point = "/mnt/silver"

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
# MAGIC # Mount gold layer

# COMMAND ----------

container_name = "gold"
account_name ="datalake202405"
mount_point = "/mnt/gold"

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
# MAGIC # End to end Demo : bronze -> silver -> Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ###Countries parquet silver

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ]
                    )

# COMMAND ----------

countries = spark.read.csv('/mnt/bronze/countries.csv',header=True, schema=countries_schema)

# COMMAND ----------

countries.printSchema()

# COMMAND ----------

silver_countries = countries.drop("SUB_REGION_ID","INTERMEDIATE_REGION_ID","ORGANIZATION_REGION_ID")

# COMMAND ----------

silver_countries.write.parquet('/mnt/silver/countries', mode= "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Region parquet silver

# COMMAND ----------

regions_schema = StructType([
                    StructField("ID", IntegerType(), False),
                    StructField("NAME", StringType(), False)
                    ]
                    )

# COMMAND ----------

regions = spark.read.csv('/mnt/bronze/country_regions.csv',header=True,schema=regions_schema)

# COMMAND ----------

silver_regions = regions.withColumnsRenamed({"NAME":"REGION_NAME",
                                             })

# COMMAND ----------

silver_regions.write.parquet('/mnt/silver/regions',mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold

# COMMAND ----------

countries = spark.read.parquet('/mnt/silver/countries')
regions = spark.read.parquet('/mnt/silver/regions')

# COMMAND ----------

gold_country_details = countries.join(regions,
                                      on= countries.REGION_ID==regions.ID,
                                      how = 'left').drop("REGION_ID","ID","COUNTRY_CODE","ISO_ALPHA2")

# COMMAND ----------

gold_country_details.write.parquet("/mnt/gold/country_data")
