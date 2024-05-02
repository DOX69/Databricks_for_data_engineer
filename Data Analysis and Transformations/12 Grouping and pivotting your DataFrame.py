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

# MAGIC %md
# MAGIC # Group by

# COMMAND ----------

# MAGIC %md
# MAGIC Agg function with sum() and countDistinct()
# MAGIC
# MAGIC we can alias them after operations in a agg()

# COMMAND ----------

# MAGIC %md
# MAGIC an aggregation on a single column 'REGION_ID' with multiple aggregation

# COMMAND ----------

from pyspark.sql.functions import *

countries.groupBy('REGION_ID')\
  .agg(sum(col('POPULATION')).alias('Sum_population'),
        countDistinct("NAME").alias("distinct_name"))\
          .display()

# COMMAND ----------

countries.groupBy('REGION_ID','SUB_REGION_ID')\
  .agg(sum(col('POPULATION')).alias('Sum_population'),
        countDistinct("NAME").alias("distinct_name"))\
          .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment
# MAGIC
# MAGIC Aggregate the DF by region_id and sub_region_id
# MAGIC
# MAGIC Display the min and max population
# MAGIC
# MAGIC sort by region_id in asc

# COMMAND ----------

countries.groupBy("REGION_ID","SUB_REGION_ID")\
    .agg(max("POPULATION").alias("max_pop"),
         min("POPULATION").alias("min_pop"))\
    .orderBy(col("REGION_ID").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pivotting

# COMMAND ----------

countries.groupBy("SUB_REGION_ID")\
    .pivot("REGION_ID")\
    .agg(sum("POPULATION")).display()
