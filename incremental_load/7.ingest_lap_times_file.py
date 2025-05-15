# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text("p_data_source","Ergast API")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

#dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df=spark.read.format("csv")\
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

final_path=f'{processed_folder_path}/lap_times'

# COMMAND ----------

final_path

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'manishct.f1_processed', 'lap_times', final_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

