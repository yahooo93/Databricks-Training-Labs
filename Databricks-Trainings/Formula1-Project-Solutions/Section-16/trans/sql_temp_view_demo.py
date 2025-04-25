# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

display(spark.sql("SELECT * FROM v_race_results WHERE race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Temporary View - Accessible only from the notebook where it was created
# MAGIC ####Global Temporary View - accessible from all notebooks attached to a cluster

# COMMAND ----------


