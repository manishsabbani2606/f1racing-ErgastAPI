# Databricks notebook source
dbutils.widgets.text("p_data_source", "Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# dbutils.widgets.text("p_file_date", "2021-03-28")
# v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

import json
res=json.loads(dbutils.notebook.run("../includes/configuration",20))

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{res['raw_folder_path']}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
# merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

final_path=f"{res['processed_folder_path']}/pit_stops"

# COMMAND ----------

final_path

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy("race_id").option("overwriteSchema",True).format("delta").save(final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql("""drop table if exists manishct.f1_processed.pit_stops""").display()
spark.sql(f"""create table manishct.f1_processed.pit_stops using delta LOCATION '{final_path}'""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_processed.pit_stops;

# COMMAND ----------

