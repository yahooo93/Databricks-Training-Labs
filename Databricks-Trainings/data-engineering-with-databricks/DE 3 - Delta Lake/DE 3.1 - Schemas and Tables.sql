-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-4c4121ee-13df-479f-be62-d59452a5f261
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Schemas and Tables on Databricks
-- MAGIC In this demonstration, you will create and explore schemas and tables.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define schemas and tables
-- MAGIC * Describe how the **`LOCATION`** keyword impacts the default storage directory
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-acb0c723-a2bf-4d00-b6cb-6e9aef114985
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Lesson Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-cc3d2766-764e-44bb-a04b-b03ae9530b6d
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Schemas
-- MAGIC Let's start by creating a schema (database).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-427db4b9-fa6c-47aa-ae70-b95087298362
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC Note that the location of the first schema (database) is in the default location under **`dbfs:/user/hive/warehouse/`** and that the schema directory is the name of the schema with the **`.db`** extension

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0fda220-4a73-419b-969f-664dd4b80024
-- MAGIC %md
-- MAGIC ## Managed Tables
-- MAGIC
-- MAGIC We will create a **managed** table (by not specifying a path for the location).
-- MAGIC
-- MAGIC We will create the table in the schema (database) we created above.
-- MAGIC
-- MAGIC Note that the table schema must be defined because there is no data from which to infer the table's columns and data types

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c422056-45b4-419d-b4a6-2c3252e82575
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE DETAIL managed_table;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED managed_table

-- COMMAND ----------

-- DBTITLE 0,--i18n-bdc6475c-1c77-46a5-9ea1-04d5a538c225
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By default, **managed** tables in a schema without the location specified will be created in the **`dbfs:/user/hive/warehouse/<schema_name>.db/`** directory.
-- MAGIC
-- MAGIC We can see that, as expected, the data and metadata for our table are stored in that location.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-507a84a5-f60f-4923-8f48-475ee3270dbd
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0b390bf4-3e3b-4d1a-bcb8-296fa1a7edb8
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Note the table's directory and its log and data files are deleted. Only the schema (database) directory remains.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md --i18n-0e4046c8-2c3a-4bab-a14a-516cc0f41eda
-- MAGIC
-- MAGIC  
-- MAGIC ## External Tables
-- MAGIC Next, we will create an **external** (unmanaged) table from sample data. 
-- MAGIC
-- MAGIC The data we are going to use are in CSV format. We want to create a Delta table with a **`LOCATION`** provided in the directory of our choice.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 

-- COMMAND ----------

-- DBTITLE 0,--i18n-6b5d7597-1fc1-4747-b5bb-07f67d806c2b
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-72f7bef4-570b-4c20-9261-b763b66b6942
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-f71374ea-db51-4a2c-8920-9f8a000850df
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-7defc948-a8e4-4019-9633-0886d653b7c6
-- MAGIC %md
-- MAGIC
-- MAGIC ## Clean up
-- MAGIC Drop the schema.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb4a8ae9-450b-479f-9e16-a76f1131bd1a
-- MAGIC %md
-- MAGIC
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
