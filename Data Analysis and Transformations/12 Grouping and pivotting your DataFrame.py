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

# COMMAND ----------

# MAGIC %md
# MAGIC # Unpivotting

# COMMAND ----------

pivot_countries = countries.groupBy("SUB_REGION_ID")\
    .pivot("REGION_ID")\
    .agg(sum("POPULATION"))

# COMMAND ----------

pivot_countries.display()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC Rename withColumsRenamed({name_old:name_new,:,:})

# COMMAND ----------

 pivot_countries= pivot_countries\
    .withColumnsRenamed({'10': 'REGION_10',
                        '20': 'REGION_20',
                        '30': 'REGION_30',
                        '40': 'REGION_40',
                        '50': 'REGION_50'},
                        )

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unpivot with expr()
# MAGIC
# MAGIC **expr("""stack(nb_columns_to_unpivot,value,column_where_value_is as (column_stack_new_col,values_stacked))""")**
# MAGIC
# MAGIC and remove the none value in values_stacked by **.na.drop(subset=values_stacked)** OR **filter('values_stacked is not null')**

# COMMAND ----------

pivot_countries\
    .select('SUB_REGION_ID',expr("""stack(5,
                                 '10',REGION_10,
                                 '20',REGION_20,
                                 '30',REGION_30,
                                 '40',REGION_40,
                                 '50',REGION_50) 
                                 as (REGION_ID,POPULATION)"""))\
                                .na.drop(subset='POPULATION').display()
