# Databricks notebook source
dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.text("p_data_source","Ergast API")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

import json
res=json.loads(dbutils.notebook.run("../includes/configuration",10))

# COMMAND ----------

res

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

res['raw_folder_path']

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC ###read the csv file data into spark dataframe 

# COMMAND ----------

from pyspark.sql.types import *

circuit_schema=StructType(fields=[StructField("circuitId", IntegerType(), True),StructField("name", StringType(), True),StructField("circuitRef", StringType(), True),StructField("location", StringType(), True),StructField("country", StringType(), True),StructField("lat", DoubleType(), True),StructField("lng", DoubleType(), True),StructField("alt", IntegerType(), True),StructField("url", StringType(), True)])

# COMMAND ----------


circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv(f"{res['raw_folder_path']}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### add selected columns only

# COMMAND ----------

from pyspark.sql.functions import col 
circuits_selected_df=circuits_df.drop(col("url"))

# COMMAND ----------

#renaming the columns 
from pyspark.sql.functions import lit
circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_Ref").withColumnRenamed("name","Name").withColumnRenamed("location","Circuit_location").withColumnRenamed("country","Country").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### data writing to processed path

# COMMAND ----------

import json 
res = json.loads(dbutils.notebook.run("../includes/configuration",20))

# COMMAND ----------

final_path=f"{res['processed_folder_path']}/circuits/"


# COMMAND ----------

final_path

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

create_catalog_table(circuits_final_df,'circuit_id',final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql(""" drop table if exists manishct.f1_processed.circuits """).display()
spark.sql(f"""create table if not exists manishct.f1_processed.circuits using delta location '{final_path}' """).display()

# COMMAND ----------

df= spark.read.format("delta").load("abfss://formula1@f1manishadls.dfs.core.windows.net/processed/circuits").display()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

