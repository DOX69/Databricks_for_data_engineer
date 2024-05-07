# Databricks notebook source
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
countries = spark.read.csv("dbfs:/FileStore/bronze/customers.csv",header=True,schema=countries_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # External table :
# MAGIC unmanaged table, you have to specify a path

# COMMAND ----------

countries.write.option('path','/FileStore/external/countries').saveAsTable('countries.countries_ext_python')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_ext_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## If you drop the table, it is deleted but the underline data persiste: 'FileStore/external/countries'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete external file

# COMMAND ----------

dbutils.fs.rm('/FileStore/external',recurse=True)
