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

# COMMAND ----------

countries_df = spark.read.csv('dbfs:/FileStore/tables/countries.csv', header='True', schema= countries_schema)

# COMMAND ----------

countries_df.write.csv('dbfs:/FileStore/tables/countries_out',header= 'True')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/countries_out', header='True').display()

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/countries_out', header='True')
df.write.csv('dbfs:/FileStore/tables/output/countries_out', header ='True')

# COMMAND ----------

# MAGIC %md
# MAGIC To overwrite

# COMMAND ----------

df.write.mode('overwrite').csv('dbfs:/FileStore/tables/output/countries_out', header ='True')

# COMMAND ----------

df.write.mode('overwrite').partitionBy("REGION_ID").csv('dbfs:/FileStore/tables/output/countries_out', header ='True')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/output/countries_out',header= True).display()

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/output/countries_out/REGION_ID=50',header= True).display()
