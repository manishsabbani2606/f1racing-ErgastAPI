# Databricks notebook source
#dbutils.widgets.text("p_data_source", "Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

#dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read.format("json") \
.schema(drivers_schema) \
.load(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)
drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

final_path=f'{processed_folder_path}/drivers'

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id"
merge_delta_data(drivers_final_df, 'manishct.f1_processed', 'drivers', final_path, merge_condition, 'driver_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

