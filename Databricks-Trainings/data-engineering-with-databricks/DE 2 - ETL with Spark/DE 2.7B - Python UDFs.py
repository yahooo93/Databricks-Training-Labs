# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7c0e5ecf-c2e4-4a89-a418-76faa15ce226
# MAGIC %md
# MAGIC
# MAGIC # Python User-Defined Functions
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Define a function
# MAGIC 1. Create and apply a UDF
# MAGIC 1. Create and register a UDF with Python decorator syntax
# MAGIC 1. Create and apply a Pandas (vectorized) UDF
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Python UDF Decorator</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Pandas UDF Decorator</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.7B

# COMMAND ----------

# DBTITLE 0,--i18n-1e94c419-dd84-4f8d-917a-019b15fc6700
# MAGIC %md
# MAGIC
# MAGIC ### User-Defined Function (UDF)
# MAGIC A custom column transformation function
# MAGIC
# MAGIC - Can’t be optimized by Catalyst Optimizer
# MAGIC - Function is serialized and sent to executors
# MAGIC - Row data is deserialized from Spark's native binary format to pass to the UDF, and the results are serialized back into Spark's native format
# MAGIC - For Python UDFs, additional interprocess communication overhead between the executor and a Python interpreter running on each worker node

# COMMAND ----------

# DBTITLE 0,--i18n-4d1eb639-23fb-42fa-9b62-c407a0ccde2d
# MAGIC %md
# MAGIC
# MAGIC For this demo, we're going to use the sales data.

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05043672-b02a-4194-ba44-75d544f6af07
# MAGIC %md
# MAGIC
# MAGIC ### Define a function
# MAGIC
# MAGIC Define a function (on the driver) to get the first letter of a string from the **`email`** field.

# COMMAND ----------

def second_letter_function(email):
    return email[1]

second_letter_function("annagray@kaufman.com")

# COMMAND ----------

# DBTITLE 0,--i18n-17f25aa9-c20f-41da-bac5-95ebb413dcd4
# MAGIC %md
# MAGIC
# MAGIC ### Create and apply UDF
# MAGIC Register the function as a UDF. This serializes the function and sends it to executors to be able to transform DataFrame records.

# COMMAND ----------

second_letter_udf = udf(second_letter_function)

# COMMAND ----------

# DBTITLE 0,--i18n-75abb6ee-291b-412f-919d-be646cf1a580
# MAGIC %md
# MAGIC
# MAGIC Apply the UDF on the **`email`** column.

# COMMAND ----------

from pyspark.sql.functions import col

display(sales_df.select(second_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-26f93012-a994-4b6a-985e-01720dbecc25
# MAGIC %md
# MAGIC
# MAGIC ### Use Decorator Syntax (Python Only)
# MAGIC
# MAGIC Alternatively, you can define and register a UDF using <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">Python decorator syntax</a>. The **`@udf`** decorator parameter is the Column datatype the function returns.
# MAGIC
# MAGIC You will no longer be able to call the local Python function (i.e., **`first_letter_udf("annagray@kaufman.com")`** will not work).
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This example also uses <a href="https://docs.python.org/3/library/typing.html" target="_blank">Python type hints</a>, which were introduced in Python 3.5. Type hints are not required for this example, but instead serve as "documentation" to help developers use the function correctly. They are used in this example to emphasize that the UDF processes one record at a time, taking a single **`str`** argument and returning a **`str`** value.

# COMMAND ----------

# Our input/output is a string
@udf("string")
def second_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

# DBTITLE 0,--i18n-4d628fe1-2d94-4d86-888d-7b9df4107dba
# MAGIC %md
# MAGIC
# MAGIC And let's use our decorator UDF here.

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(second_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-3ae354c0-0b10-4e8c-8cf6-da68e8fba9f2
# MAGIC %md
# MAGIC
# MAGIC ### Pandas/Vectorized UDFs
# MAGIC
# MAGIC Pandas UDFs are available in Python to improve the efficiency of UDFs. Pandas UDFs utilize Apache Arrow to speed up computation.
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Blog post</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentation</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC
# MAGIC The user-defined functions are executed using: 
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>, an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost
# MAGIC * Pandas inside the function, to work with Pandas instances and APIs
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> As of Spark 3.0, you should **always** define your Pandas UDF using Python type hints.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-9a2fb1b1-8060-4e50-a759-f30dc73ce1a1
# MAGIC %md
# MAGIC
# MAGIC We can register these Pandas UDFs to the SQL namespace.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# DBTITLE 0,--i18n-5e506b8d-a488-4373-af9a-9ebb14834b1b
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
