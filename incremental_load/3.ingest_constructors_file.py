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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source))\
                                              .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

final_path=f'{processed_folder_path}/constructors'

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").option("overwriteSchema",True).format("delta").save(final_path)

# COMMAND ----------

merge_condition = "tgt.constructor_id = src.constructor_id"
merge_delta_data(constructor_final_df, 'manishct.f1_processed', 'constructors', final_path, merge_condition, 'constructor_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

