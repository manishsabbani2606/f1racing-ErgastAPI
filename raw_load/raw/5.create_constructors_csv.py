# Databricks notebook source
dbutils.widgets.text("p_data_source", "Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

import json
res=json.loads(dbutils.notebook.run("../includes/configuration",20))

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

from pyspark.sql.types import *
constructors_schema = StructType(fields=[StructField("constructorId",IntegerType(),True), StructField("constructorRef",StringType(),True), StructField("name",StringType(),True), StructField("nationality",StringType(),True), StructField("url",StringType(),True)])

# COMMAND ----------

constructors_df=spark.read\
    .option("header",True)\
        .schema(constructors_schema)\
        .json(f"{res['raw_folder_path']}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_final_df=add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

final_path=f"{res['processed_folder_path']}/constructors/"

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").save(final_path)

# COMMAND ----------

spark.sql("create database if not exists manishct.f1_processed")
spark.sql("""drop table if exists manishct.f1_processed.constructors""").display()
spark.sql(f"create table manishct.f1_processed.constructors using delta location '{final_path}'")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manishct.f1_processed.constructors;

# COMMAND ----------

