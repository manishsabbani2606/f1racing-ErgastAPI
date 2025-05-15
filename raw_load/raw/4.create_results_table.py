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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{res['raw_folder_path']}/results.json")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) 

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df, 'race_id')

# COMMAND ----------

final_path=f"{res['processed_folder_path']}/results"


# COMMAND ----------

final_path

# COMMAND ----------

results_deduped_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql("""drop table if exists manishct.f1_processed.results""").display()
spark.sql(f"""create table manishct.f1_processed.results using delta LOCATION '{final_path}'""").display()

# COMMAND ----------

#How to overwrite the data with new data
