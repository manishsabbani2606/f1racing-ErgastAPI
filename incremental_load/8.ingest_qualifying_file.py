# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text("p_data_source","Ergast API")
#dbutils.widgets.text("p_file_date","2021-03-21")
v_data_source=dbutils.widgets.get("p_data_source")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------


qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit
final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

final_path=f'{processed_folder_path}/qualifying'
final_path

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'manishct.f1_processed', 'qualifying', final_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

