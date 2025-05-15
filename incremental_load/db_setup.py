# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists manishct.f1_processed cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists manishct.f1_processed managed location
# MAGIC 'abfss://formula1@f1manishadls.dfs.core.windows.net/processed';
# MAGIC

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS manishct.f1_processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_processed.circuits;

# COMMAND ----------

