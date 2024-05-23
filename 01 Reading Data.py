# Databricks notebook source
countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header = 'True')

# COMMAND ----------

countries_df.display()

# COMMAND ----------

spark.read.options(header = 'True').csv('dbfs:/FileStore/tables/countries.csv').display()

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

countries_df = spark.read.options(header = 'True', inferSchema = 'True').csv('dbfs:/FileStore/tables/countries.csv')

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, DoubleType, StructType
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

countries_df = spark.read.csv('dbfs:/FileStore/tables/countries.csv', header='True', schema= countries_schema)

# COMMAND ----------

countries_sl_json = spark.read.json('dbfs:/FileStore/tables/countries_single_line.json')

# COMMAND ----------

countries_sl_json.display()

# COMMAND ----------

countries_ml_json = spark.read.options(multiLine ='True').json('dbfs:/FileStore/tables/countries_single_line.json')

# COMMAND ----------

countries_ml_json.display()

# COMMAND ----------

countries_txt = spark.read.csv('dbfs:/FileStore/tables/countries.txt', header= 'True', sep = '\t', schema= countries_schema)

# COMMAND ----------

countries_txt.limit(10).display()
