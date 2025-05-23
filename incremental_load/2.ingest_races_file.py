# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

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

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'),col("data_source"),col('file_date'))

# COMMAND ----------

races_selected_df.select("race_year").distinct().display()

# COMMAND ----------

final_path=f'{processed_folder_path}/races/'

# COMMAND ----------

final_path

# COMMAND ----------


#races_selected_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(final_path)

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id"
merge_delta_data(races_selected_df, 'manishct.f1_processed', 'races', final_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

