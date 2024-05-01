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

countries = countries.withColumn("current_timestamp",current_timestamp())

# COMMAND ----------

countries.select(year(countries['current_timestamp'])).limit(5).display()

# COMMAND ----------

countries = countries.withColumn("today",lit('01-05-2024'))

# COMMAND ----------

countries.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC to_date function
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

# COMMAND ----------

countries_2 = countries.withColumn("today",to_date(col("today"),'dd-MM-yyyy'))
