# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("p_data_source","Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *

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

from pyspark.sql.functions import lit,concat,col,to_timestamp

# COMMAND ----------

import json
res = json.loads(dbutils.notebook.run("../includes/configuration",10))

# COMMAND ----------

races_df=spark.read.option("header",True).schema(races_schema).csv(f"{res['raw_folder_path']}/races.csv")

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

races_with_ingestion_date_df=add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

#select only required columns
races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('timestamp'))

# COMMAND ----------

races_selected_df.display()

# COMMAND ----------

#writing the output to processed container as parquet format
final_path = f"{res['processed_folder_path']}/races"

# COMMAND ----------

final_path

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").partitionBy("race_year").save(final_path)


# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql(""" drop table if exists manishct.f1_processed.races """).display()
spark.sql(f""" create table manishct.f1_processed.races using delta location '{final_path}'""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_processed.races;

# COMMAND ----------

