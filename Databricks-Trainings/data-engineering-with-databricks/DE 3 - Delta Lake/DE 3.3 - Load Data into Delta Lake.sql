-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a518bafd-59bd-4b23-95ee-5de2344023e4
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC # Loading Data into Delta Lake
-- MAGIC Delta Lake tables provide ACID compliant updates to tables backed by data files in cloud object storage.
-- MAGIC
-- MAGIC In this notebook, we'll explore SQL syntax to process updates with Delta Lake. While many operations are standard SQL, slight variations exist to accommodate Spark and Delta Lake execution.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Overwrite data tables using **`INSERT OVERWRITE`**
-- MAGIC - Append to a table using **`INSERT INTO`**
-- MAGIC - Append, update, and delete from a table using **`MERGE INTO`**
-- MAGIC - Ingest data incrementally into tables using **`COPY INTO`**

-- COMMAND ----------

-- DBTITLE 0,--i18n-af486892-a86c-4ef2-9996-2ace24b5737c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-04a35896-fb09-4a99-8d00-313480e5c6a1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC ## Complete Overwrites
-- MAGIC
-- MAGIC We can use overwrites to atomically replace all of the data in a table. There are multiple benefits to overwriting tables instead of deleting and recreating tables:
-- MAGIC - Overwriting a table is much faster because it doesn’t need to list the directory recursively or delete any files.
-- MAGIC - The old version of the table still exists; can easily retrieve the old data using Time Travel.
-- MAGIC - It’s an atomic operation. Concurrent queries can still read the table while you are deleting the table.
-- MAGIC - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC
-- MAGIC Spark SQL provides two easy methods to accomplish complete overwrites.
-- MAGIC
-- MAGIC Some students may have noticed previous lesson on CTAS statements actually used CRAS statements (to avoid potential errors if a cell was run multiple times).
-- MAGIC
-- MAGIC **`CREATE OR REPLACE TABLE`** (CRAS) statements fully replace the contents of a table each time they execute.

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-8f767697-33e6-4b5b-ac09-862076f77033
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Reviewing the table history shows a previous version of this table was replaced.

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb68d513-240c-41e1-902c-3c3add9c0a75
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`** provides a nearly identical outcome as above: data in the target table will be replaced by data from the query. 
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**:
-- MAGIC
-- MAGIC - Can only overwrite an existing table, not create a new one like our CRAS statement
-- MAGIC - Can overwrite only with new records that match the current table schema -- and thus can be a "safer" technique for overwriting an existing table without disrupting downstream consumers
-- MAGIC - Can overwrite individual partitions

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`

-- COMMAND ----------

-- DBTITLE 0,--i18n-cfefb85f-f762-43db-be9b-cb536a06c842
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that different metrics are displayed than a CRAS statement; the table history also records the operation differently.

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

select * from 
(select *, 'old' as file from sales timestamp as of '2024-07-09T11:32:39.000+00:00'
union all
select *, 'new' as file from sales)
order by order_id

-- COMMAND ----------

-- DBTITLE 0,--i18n-40769b04-c72b-4740-9d27-ea2d1b8700f3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC A primary difference here has to do with how Delta Lake enforces schema on write.
-- MAGIC
-- MAGIC Whereas a CRAS statement will allow us to completely redefine the contents of our target table, **`INSERT OVERWRITE`** will fail if we try to change our schema (unless we provide optional settings). 
-- MAGIC
-- MAGIC Uncomment and run the cell below to generate an expected error message.

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-ceb78e46-6362-4c3b-b63d-54f42d38dd1f
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Append Rows
-- MAGIC
-- MAGIC We can use **`INSERT INTO`** to atomically append new rows to an existing Delta table. This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.
-- MAGIC
-- MAGIC Append new sale records to the **`sales`** table using **`INSERT INTO`**.

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-171f9cf2-e0e5-4f8d-9dc7-bf4770b6d8e5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Note that **`INSERT INTO`** does not have any built-in guarantees to prevent inserting the same records multiple times. Re-executing the above cell would write the same records to the target table, resulting in duplicate records.

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ad4ab1f-a7c1-439d-852e-ff504dd16307
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Merge Updates
-- MAGIC
-- MAGIC You can upsert data from a source table, view, or DataFrame into a target Delta table using the **`MERGE`** SQL operation. Delta Lake supports inserts, updates and deletes in **`MERGE`**, and supports extended syntax beyond the SQL standards to facilitate advanced use cases.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC We will use the **`MERGE`** operation to update historic users data with updated emails and new users.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-4732ea19-2857-45fe-9ca2-c2475015ef47
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC The main benefits of **`MERGE`**:
-- MAGIC * updates, inserts, and deletes are completed as a single transaction
-- MAGIC * multiple conditionals can be added in addition to matching fields
-- MAGIC * provides extensive options for implementing custom logic
-- MAGIC
-- MAGIC Below, we'll only update records if the current row has a **`NULL`** email and the new row does not. 
-- MAGIC
-- MAGIC All unmatched records from the new batch will be inserted.

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-5cae1734-7eaf-4a53-a9b5-c093a8d73cc9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that we explicitly specify the behavior of this function for both the **`MATCHED`** and **`NOT MATCHED`** conditions; the example demonstrated here is just an example of logic that can be applied, rather than indicative of all **`MERGE`** behavior.

-- COMMAND ----------

-- DBTITLE 0,--i18n-d7d2c7fd-2c83-4ed2-aa78-c37992751881
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Insert-Only Merge for Deduplication
-- MAGIC
-- MAGIC A common ETL use case is to collect logs or other every-appending datasets into a Delta table through a series of append operations. 
-- MAGIC
-- MAGIC Many source systems can generate duplicate records. With merge, you can avoid inserting the duplicate records by performing an insert-only merge.
-- MAGIC
-- MAGIC This optimized command uses the same **`MERGE`** syntax but only provided a **`WHEN NOT MATCHED`** clause.
-- MAGIC
-- MAGIC Below, we use this to confirm that records with the same **`user_id`** and **`event_timestamp`** aren't already in the **`events`** table.

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-75891a95-c6f2-4f00-b30e-3df2df858c7c
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Load Incrementally
-- MAGIC
-- MAGIC **`COPY INTO`** provides SQL engineers an idempotent option to incrementally ingest data from external systems.
-- MAGIC
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC
-- MAGIC This operation is potentially much cheaper than full table scans for data that grows predictably.
-- MAGIC
-- MAGIC While here we'll show simple execution on a static directory, the real value is in multiple executions over time picking up new files in the source automatically.

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd65fe71-cdaf-47a8-85ec-fa9769c11708
-- MAGIC %md
-- MAGIC
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
