# Databricks notebook source
# Reading in the countries csv file as a Dataframe
countries_path = '/FileStore/tables/countries.csv'

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

countries = spark.read.csv(path=countries_path, header = True, schema=countries_schema)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Upper

# COMMAND ----------

countries.select(upper(col("name")).alias("Upper_name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Initcap

# COMMAND ----------

countries.select(initcap(col("name")).alias("initcap_name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC length

# COMMAND ----------

countries.select(length(col("name")).alias("length")).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Concat

# COMMAND ----------

countries.select(concat_ws(" - ", col("Name"),col("country_code")).alias("concat")).limit(5).display()
