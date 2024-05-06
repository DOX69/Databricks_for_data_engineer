# Databricks notebook source
# MAGIC %md
# MAGIC # Create database in hive metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS countries ;

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop database

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS countries ;
