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

from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC expr() can operate sql function like left, case when, etc

# COMMAND ----------

countries.select(expr('left(NAME,2) as 2_country')).distinct().limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC if the population is greatr than 100million return large, medium for 50 million above and small otherwise

# COMMAND ----------

countries.select("name",
                 "POPULATION",
                 expr("case when POPULATION>100000000 then 'Large' when POPULATION between 50000000 and 100000000 then 'Medium' else 'Small' end as class_population "))\
                .limit(10000)\
                .display()

# COMMAND ----------

countries.withColumn("area_class",
                 expr("case when AREA_KM2>1000000 then 'Large' when AREA_KM2 between 300000 and 1000000 then 'Medium' else 'Small' end"))\
                .display()
