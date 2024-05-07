# Databricks notebook source
# MAGIC %md
# MAGIC # Use data catalog UI to import an create a managed table 

# COMMAND ----------

countries = spark.read.table("hive_metastore.countries.countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use countries

# COMMAND ----------

# MAGIC %md
# MAGIC # Managed table 
# MAGIC A managed table is a table and metadata that is managed by databricks file system
# MAGIC
# MAGIC Les données et les métadata sont stockés et géré par DBFS
# MAGIC
# MAGIC Plus adapté pour les data analystes qui utilisent SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table countries

# COMMAND ----------

# MAGIC %md
# MAGIC # Use pyspark to create a managed table

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

countries = spark.read.csv("dbfs:/FileStore/bronze/customers.csv",header=True,schema=countries_schema)

# COMMAND ----------

# display(dbutils.fs.ls('dbfs:/FileStore/bronze/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use default

# COMMAND ----------

countries.write.saveAsTable('countries.countries_default_loc')

# COMMAND ----------

# MAGIC %md
# MAGIC # Use SQL to create a managed table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an empty table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE countries.countries_mt_empty
# MAGIC (country_id int,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC country_code string,
# MAGIC iso_alpha_2 string,
# MAGIC capital string,
# MAGIC population int,
# MAGIC area_km2 int,
# MAGIC region_id int,
# MAGIC sub_region_id int,  
# MAGIC intermediate_region_id int,
# MAGIC organization_region_id int)
# MAGIC USING CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a table by copy an other one

# COMMAND ----------

# MAGIC %sql
# MAGIC create table countries.countries_copy as select * from countries.countries_mt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop all table in countries database

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- DROP TABLE countries.countries_copy

# COMMAND ----------

# MAGIC %md
# MAGIC # You can specify location using LOCATION command 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database <Datbase name>
# MAGIC LOCATION 'location/file'
