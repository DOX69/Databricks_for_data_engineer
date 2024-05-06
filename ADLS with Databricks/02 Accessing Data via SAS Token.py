# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.datalake202405.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake202405.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake202405.dfs.core.windows.net","sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-06T16:21:46Z&st=2024-05-06T08:21:46Z&spr=https&sig=K3siDGomQvFm%2FNRhyVDLdNfoGdKUs2e29eQOPqmvdEY%3D")

# COMMAND ----------

spark.read.csv("abfss://bronze@datalake202405.dfs.core.windows.net/country_regions.csv", header=True).display()
