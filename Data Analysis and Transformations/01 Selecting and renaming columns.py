# Databricks notebook source
# MAGIC %md
# MAGIC #Country BD

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema

# COMMAND ----------

# MAGIC %md
# MAGIC schema = StructType(
# MAGIC [
# MAGIC   StructField("name",IntegerType(),False)
# MAGIC   ]
# MAGIC )

# COMMAND ----------

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


# COMMAND ----------

# MAGIC %md
# MAGIC Import country csv

# COMMAND ----------

countries = spark.read.csv(path=countries_path,header= True, schema=countries_schema)

# COMMAND ----------

countries.describe().display()

# COMMAND ----------

countries.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select columns

# COMMAND ----------

countries.select("name","nationality","country_code").limit(10).display()

# COMMAND ----------

countries.select(countries['name'],countries['nationality'],countries['country_code']).limit(5).display()

# COMMAND ----------

from pyspark.sql.functions import col
countries.select(col('name'),col('population')).limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename

# COMMAND ----------

countries.select(countries['name'].alias("country_name")).display()

# COMMAND ----------

countries.withColumnRenamed("name","countries_name").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Region DB

# COMMAND ----------

region_path = '/FileStore/tables/country_regions.csv'

region_schema = StructType(
    [StructField("ID",IntegerType(),False),
     StructField("NAME",StringType(),False)
     ]
)

region = spark.read.csv(path=region_path,header=True,schema=region_schema).withColumnRenamed("NAME","Continent")

# COMMAND ----------

region.limit(10).display()

# COMMAND ----------


