# Databricks notebook source
# MAGIC %md
# MAGIC # I- Mount the 'employees' container from ADLS to the DBFS
# MAGIC _Ideally use secret scopes to ensure that your ID's and secrets are not exposed._

# COMMAND ----------

# MAGIC %md
# MAGIC Before these step, go to your ressource group >> your container>> create a new folder employees/[bronze,silver,gold]

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to get key
# MAGIC Go to your ***"App Registration"*** in azure or create one >> overview>> copy app id and tenant id
# MAGIC
# MAGIC For the secret: app registration >> certificates & secret >> create one
# MAGIC
# MAGIC a54c2834-3bff-4943-8cfd-bbebaee25310
# MAGIC ef45c45c-4142-434c-b13e-f7ac7c104097
# MAGIC 82a14ab0-988f-4b91-9d09-398743ce3ad8

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get secrets 
# MAGIC to create one : ***"Key Vault"*** 
# MAGIC
# MAGIC to find yours (if exists) : go to your ressource group

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope='dbx-secrets-202405',key="tenant-id")
application_id= dbutils.secrets.get(scope='dbx-secrets-202405',key="application-id")
secret = dbutils.secrets.get(scope='dbx-secrets-202405',key="secret")

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount using secrets

# COMMAND ----------

container_name = "employees"
account_name ="datalake202405"
mount_point = "/mnt/employees"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC # II- Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC All tables must be PARQUET file type. Columns must not be nullable except for MANAGER_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Employees

# COMMAND ----------

from pyspark.sql.types import *
employees_schema = StructType([
    StructField("EMPLOYEE_ID",IntegerType(),False),
    StructField("FIRST_NAME",StringType(),False),
    StructField("LAST_NAME",StringType(),False),
    StructField("EMAIL",StringType(),False),
    StructField("PHONE_NUMBER",StringType(),False),
    StructField("HIRE_DATE",StringType(),False),
    StructField("JOB_ID",IntegerType(),False),
    StructField("SALARY",IntegerType(),False),
    StructField("MANAGER_ID",IntegerType(),True),
    StructField("DEPARTMENT_ID",IntegerType(),False),
])

# COMMAND ----------

employees = spark.read.csv('/mnt/employees/bronze/employees.csv',header=True,schema=employees_schema)

# COMMAND ----------

from pyspark.sql.functions import *
silver_employees = employees.select(
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    to_date(col("HIRE_DATE"),"dd/MM/yyyy").alias("HIRE_DATE"),
    "JOB_ID",
    "SALARY",
    "MANAGER_ID",
    "DEPARTMENT_ID"
    )

# COMMAND ----------

silver_employees.write.parquet('/mnt/employees/silver/employees',mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Departments

# COMMAND ----------

departments_schema = StructType([
    StructField("DEPARTMENT_ID",IntegerType(),False),
    StructField("DEPARTMENT_NAME",StringType(),False),
    StructField("MANAGER_ID",IntegerType(),False),
    StructField("LOCATION_ID",IntegerType(),False),
])

# COMMAND ----------

spark.read.csv('/mnt/employees/bronze/departments.csv',header=True,schema=departments_schema)\
    .select("DEPARTMENT_ID","DEPARTMENT_NAME")\
    .write.parquet('/mnt/employees/silver/departments',mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## countries

# COMMAND ----------

countries_schema = StructType([
    StructField("COUNTRY_ID",IntegerType(),False),
    StructField("COUNTRY_NAME",StringType(),False),
])

# COMMAND ----------

spark.read.csv('/mnt/employees/bronze/countries.csv',header=True,schema=countries_schema)\
    .write.parquet('/mnt/employees/silver/countries',mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC # III- Gold layer
# MAGIC
# MAGIC All tables must be PARQUET files type. Columns must not be nullable except for MANAGER_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Employees

# COMMAND ----------

departments = spark.read.parquet('/mnt/employees/silver/departments')
employees = spark.read.parquet('/mnt/employees/silver/employees')

# COMMAND ----------

gold_employees = employees.join(departments,
                                on = "DEPARTMENT_ID",
                                how = "left")\
                           .withColumn("FULL_NAME",concat_ws(' ',col("FIRST_NAME"),col("LAST_NAME")))\
                           .select("EMPLOYEE_ID","FULL_NAME","HIRE_DATE","JOB_ID","SALARY","MANAGER_ID","DEPARTMENT_NAME")

# COMMAND ----------

gold_employees.drop("MANAGER_ID").write.parquet("/mnt/employees/gold/employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus : Manager table

# COMMAND ----------

gold_employees.createOrReplaceTempView("employees")

# COMMAND ----------

gold_manager = spark.sql("""
with manager as (
  select
  MANAGER_ID,
  COUNT(MANAGER_ID) NB_EMPLOYEES_MINUS_1
  FROM employees
  group by 1
  HAVING COUNT(MANAGER_ID)>0
)
select 
m.MANAGER_ID,
e.EMPLOYEE_ID,
e.FULL_NAME,
e.SALARY as MANAGER_SALARY,
e.DEPARTMENT_NAME,
cast(ROUND(AVG(e2.SALARY),0) as int) AVG_EMPLOYEE_SALARY
from manager m
inner join employees e
on m.manager_id=e.employee_id
-- get average salary of manager's employees
left join employees e2
on m.MANAGER_ID=e2.MANAGER_ID
group by all
""")

# COMMAND ----------

gold_manager.write.parquet("/mnt/employees/gold/manager",mode = "overwirte")

# COMMAND ----------

gold_manager.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # IV- Create a database in the hive metastore called 'employees' (and manager)
# MAGIC It should contain an external table from gold layer

# COMMAND ----------


