# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-da4e23df-1911-4f58-9030-65da697d7b61
# MAGIC %md
# MAGIC # Spark SQL Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Create a DataFrame from the **`events`** table
# MAGIC 1. Display the DataFrame and inspect its schema
# MAGIC 1. Apply transformations to filter and sort **`macOS`** events
# MAGIC 1. Count results and take the first 5 rows
# MAGIC 1. Create the same DataFrame using a SQL query
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> actions: **`select`**, **`count`**, **`take`**
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.02L

# COMMAND ----------

# DBTITLE 0,--i18n-e0f3f405-8c97-46d1-8550-fb8ff14e5bd6
# MAGIC %md
# MAGIC
# MAGIC ### 1. Create a DataFrame from the **`events`** table
# MAGIC - Use SparkSession to create a DataFrame from the **`events`** table

# COMMAND ----------

# TODO
events_df = spark.table("events")

# COMMAND ----------

# DBTITLE 0,--i18n-fb5458a0-b475-4d77-b06b-63bb9a18d586
# MAGIC %md
# MAGIC
# MAGIC ### 2. Display DataFrame and inspect schema
# MAGIC - Use methods above to inspect DataFrame contents and schema

# COMMAND ----------

display(events_df)

# COMMAND ----------

events_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-76adfcb2-f182-485c-becd-9e569d4148b6
# MAGIC %md
# MAGIC
# MAGIC ### 3. Apply transformations to filter and sort **`macOS`** events
# MAGIC - Filter for rows where **`device`** is **`macOS`**
# MAGIC - Sort rows by **`event_timestamp`**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Use single and double quotes in your filter SQL expression

# COMMAND ----------

# TODO
mac_df = (events_df
          .filter(events_df.device == "macOS")
          .sort("event_timestamp")
         )

# COMMAND ----------

display(mac_df)

# COMMAND ----------

# DBTITLE 0,--i18n-81f8748d-a154-468b-b02e-ef1a1b6b2ba8
# MAGIC %md
# MAGIC
# MAGIC ### 4. Count results and take first 5 rows
# MAGIC - Use DataFrame actions to count and take rows

# COMMAND ----------

# TODO
num_rows = mac_df.count()
rows = mac_df.take(5)

# COMMAND ----------

rows

# COMMAND ----------

rows2 = mac_df.limit(2).collect()

# COMMAND ----------

rows2

# COMMAND ----------

# DBTITLE 0,--i18n-4e340689-5d23-499a-9cd2-92509a646de6
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 97150)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cbb03650-db3b-42b3-96ee-54ea9b287ab5
# MAGIC %md
# MAGIC
# MAGIC ### 5. Create the same DataFrame using SQL query
# MAGIC - Use SparkSession to run a SQL query on the **`events`** table
# MAGIC - Use SQL commands to write the same filter and sort query used earlier

# COMMAND ----------

# TODO
mac_sql_df = spark.sql(
"""
SELECT * FROM EVENTS
WHERE DEVICE = 'macOS'
SORT BY event_timestamp
"""

)

display(mac_sql_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from events where event_timestamp = '1592539226602157'

# COMMAND ----------

# DBTITLE 0,--i18n-1d203e4e-e835-4778-a245-daf30cc9f4bc
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**
# MAGIC - You should only see **`macOS`** values in the **`device`** column
# MAGIC - The fifth row should be an event with timestamp **`1592539226602157`**

# COMMAND ----------

verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592540419446946), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-5b3843b3-e615-4dc6-aec4-c8ce4d684464
# MAGIC %md
# MAGIC
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
