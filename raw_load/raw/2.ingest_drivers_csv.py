# Databricks notebook source
dbutils.widgets.text("p_data_source","Ergast API")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),StructField("surname",StringType(),True)])


# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),False),StructField("driverRef",StringType(),True),StructField("number",IntegerType(),True),StructField("code",StringType(),True),StructField("name",name_schema),StructField("dob",DateType(),True),StructField("nationality",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

import json
res=json.loads(dbutils.notebook.run("../includes/configuration",15))

# COMMAND ----------

res

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{res['raw_folder_path']}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

final_path=f'{res["processed_folder_path"]}/drivers/'
#final_path=f"{res['processed_folder_path']}/circuits/"

# COMMAND ----------

create_catalog_table(drivers_final_df,"driver_id", final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql("""drop table if exists manishct.f1_processed.drivers""").display()
spark.sql(f"""create table manishct.f1_processed.drivers using delta LOCATION '{final_path}'""").display()

# COMMAND ----------

