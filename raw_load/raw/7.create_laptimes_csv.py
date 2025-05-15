# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

import json
res=json.loads(dbutils.notebook.run("../includes/configuration",20))

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{res['raw_folder_path']}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

lap_times_with_ingestion_date_df=add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit
final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) 

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')
# merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
# merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

final_path='abfss://formula1@f1manishadls.dfs.core.windows.net/processed/lap_times/'

# COMMAND ----------

final_path

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").partitionBy("race_id").option("overwriteSchema", "true").save(final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql("""drop table if exists manishct.f1_processed.lap_times""").display()
spark.sql(f"""create table manishct.f1_processed.lap_times using delta LOCATION '{final_path}'""").display()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

