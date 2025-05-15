# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text("p_data_source","Ergast API")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

#dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the file using pyspark data frame reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count specific columns 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df=circuits_df.drop("url")
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming the column names as per convinence

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Then add the ingestion date

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

final_path = f"{processed_folder_path}/circuits/"

# COMMAND ----------

final_path

# COMMAND ----------

#circuits_final_df.write.mode("append").option("overwriteSchema",True).option("mergeSchema",True).format("delta").partitionBy("circuit_id").save(final_path)

# COMMAND ----------

merge_condition = "tgt.circuit_id = src.circuit_id"
merge_delta_data(circuits_final_df, 'manishct.f1_processed', 'circuits', final_path, merge_condition, 'circuit_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

