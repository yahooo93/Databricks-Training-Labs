# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

demo_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

display(demo_df)

# COMMAND ----------


