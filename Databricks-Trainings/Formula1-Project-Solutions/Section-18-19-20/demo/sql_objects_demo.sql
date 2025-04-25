-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo;
SELECT current_database();

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_python_ext_py")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python_ext_py;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year = 2020

-- COMMAND ----------

CREATE EXTERNAL TABLE race_results_ext_sql
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2020
LOCATION "/mnt/formula1dl0912/presentation/race_results_ext/sql";

-- COMMAND ----------

CREATE TABLE race_results_sql
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2020
LOCATION "/mnt/formula1dl0912/presentation/race_results_ext/sql";

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

select * from race_results_sql

-- COMMAND ----------

describe extended demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


