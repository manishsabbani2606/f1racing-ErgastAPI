# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists manishct managed location 'abfss://formula1@f1manishadls.dfs.core.windows.net/catalog/manishct';

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists manishct.f1_raw managed location 'abfss://formula1@f1manishadls.dfs.core.windows.net/catalog/manishct/f1_raw'

# COMMAND ----------

# MAGIC %md
# MAGIC #circuitsTable

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.circuits;
# MAGIC create table if not exists manishct.f1_raw.circuits (
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat STRING,
# MAGIC   lng STRING,
# MAGIC   alt STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC using csv
# MAGIC options ( path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/circuits.csv', header True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC #Races

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.races;
# MAGIC create table if not exists manishct.f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC using csv
# MAGIC options (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/races.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manishct.f1_raw.races;

# COMMAND ----------

# MAGIC %md
# MAGIC #ConstructorsTable

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS manishct.f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS manishct.f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC   )
# MAGIC   using csv
# MAGIC OPTIONS (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/constructors.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC ##ResultsTable

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.results;
# MAGIC create table if not exists manishct.f1_raw.results(
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   grid INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   positionOrder INT,
# MAGIC   points DECIMAL(10,2),
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap INT,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed STRING,
# MAGIC   statusId INT,
# MAGIC   url STRING
# MAGIC   )
# MAGIC   using csv
# MAGIC   options (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/results.csv', header True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.results;

# COMMAND ----------

# MAGIC %md
# MAGIC #pitstops Table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists pit_stops;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.pit_stops;
# MAGIC CREATE table if not exists manishct.f1_raw.pit_stops(
# MAGIC     raceId INT,
# MAGIC     driverId INT,
# MAGIC     stop INT,
# MAGIC     lap INT,
# MAGIC     time STRING,
# MAGIC     duration STRING,
# MAGIC     milliseconds INT,
# MAGIC     url STRING
# MAGIC )
# MAGIC using json
# MAGIC options (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/pit_stops.json', multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.qualifying;
# MAGIC create table if not exists manishct.f1_raw.qualifying(
# MAGIC     qualifyId INT,
# MAGIC     raceId INT,
# MAGIC     driverId INT,
# MAGIC     constructorId INT,
# MAGIC     number INT,
# MAGIC     position INT,
# MAGIC     q1 STRING,
# MAGIC     q2 STRING,
# MAGIC     q3 STRING
# MAGIC     )
# MAGIC     using json
# MAGIC     options (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/qualifying', multiLine True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.qualifying;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.lap_times;
# MAGIC create table if not exists manishct.f1_raw.lap_times(
# MAGIC     resultId INT,
# MAGIC     raceId INT,
# MAGIC     driverId INT,
# MAGIC     lap INT,
# MAGIC     position INT,
# MAGIC     time STRING
# MAGIC     )
# MAGIC     using csv
# MAGIC     OPTIONS (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/lap_times')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists manishct.f1_raw.drivers;
# MAGIC create table if not exists manishct.f1_raw.drivers(
# MAGIC     driverId INT,
# MAGIC     driverRef STRING,
# MAGIC     number INT,
# MAGIC     code STRING,
# MAGIC     forename STRING,
# MAGIC     surname STRING,
# MAGIC     dob STRING,
# MAGIC     nationality STRING,
# MAGIC     url STRING
# MAGIC     )
# MAGIC     using csv
# MAGIC     options (path 'abfss://formula1@f1manishadls.dfs.core.windows.net/raw/drivers.csv')
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manishct.f1_raw.drivers;

# COMMAND ----------

import json
res = json.loads(dbutils.notebook.run("../includes/configuration",15))

# COMMAND ----------

