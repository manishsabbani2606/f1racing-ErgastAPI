# Databricks notebook source
raw_folder_path = "abfss://formula1@f1manishadls.dfs.core.windows.net/raw"
processed_folder_path = "abfss://formula1@f1manishadls.dfs.core.windows.net/processed"
presentation_folder_path = "abfss://formula1@f1manishadls.dfs.core.windows.net/presentation"    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the path variables
# MAGIC ###Then exit the notebook,(by dbutils), Then json will dump the values of path variables 

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "raw_folder_path": raw_folder_path,
    "processed_folder_path": processed_folder_path,
    "presentation_folder_path": presentation_folder_path
}))

# COMMAND ----------

