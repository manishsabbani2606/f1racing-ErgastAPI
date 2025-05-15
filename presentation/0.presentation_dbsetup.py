# Databricks notebook source
# MAGIC %sql
# MAGIC drop database manishct.f1_presentation cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists manishct.f1_presentation managed location
# MAGIC 'abfss://formula1@f1manishadls.dfs.core.windows.net/presentation'

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS manishct.f1_processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_processed.circuits;
# MAGIC select distinct file_date from manishct.f1_processed.circuits;

# COMMAND ----------



# COMMAND ----------

