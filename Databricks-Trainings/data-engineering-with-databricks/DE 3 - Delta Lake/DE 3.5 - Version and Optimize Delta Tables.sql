-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d527322-1a21-4a91-bc34-e7957e052a75
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Versioning, Optimization, Vacuuming in Delta Lake
-- MAGIC
-- MAGIC Now that you feel comfortable performing basic data tasks with Delta Lake, we can discuss a few features unique to Delta Lake.
-- MAGIC
-- MAGIC Note that while some of the keywords used here aren't part of standard ANSI SQL, all Delta Lake operations can be run on Databricks using SQL
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use **`OPTIMIZE`** to compact small files
-- MAGIC * Use **`ZORDER`** to index tables
-- MAGIC * Describe the directory structure of Delta Lake files
-- MAGIC * Review a history of table transactions
-- MAGIC * Query and roll back to previous table version
-- MAGIC * Clean up stale data files with **`VACUUM`**
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef1115dd-7242-476a-a929-a16aa09ce9c1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and schema that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.2 

-- COMMAND ----------

-- DBTITLE 0,--i18n-b10dbe8f-e936-4ca3-9d1e-8b471c4bc162
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Creating a Delta Table with History
-- MAGIC
-- MAGIC As you're waiting for this query to run, see if you can identify the total number of transactions being executed.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

select * 
from students

-- COMMAND ----------

-- DBTITLE 0,--i18n-e932d675-aa26-42a7-9b55-654ac9896dab
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Examine Table Details
-- MAGIC
-- MAGIC Databricks uses a Hive metastore by default to register schemas, tables, and views.
-- MAGIC
-- MAGIC Using **`DESCRIBE EXTENDED`** allows us to see important metadata about our table.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be5873-30b3-4e7e-9333-2c1e6f1cbe25
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** is another command that allows us to explore table metadata.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd7b24fa-7a2d-4f31-ab7c-fcc28d617d75
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note the **`Location`** field.
-- MAGIC
-- MAGIC While we've so far been thinking about our table as just a relational entity within a schema, a Delta Lake table is actually backed by a collection of files stored in cloud object storage.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ff9d64a-f0c4-4ee6-a007-888d4d082abe
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Explore Delta Lake Files
-- MAGIC
-- MAGIC We can see the files backing our Delta Lake table by using a Databricks Utilities function.
-- MAGIC
-- MAGIC **NOTE**: It's not important right now to know everything about these files to work with Delta Lake, but it will help you gain a greater appreciation for how the technology is implemented.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-1a84bb11-649d-463b-85ed-0125dc599524
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that our directory contains a number of Parquet data files and a directory named **`_delta_log`**.
-- MAGIC
-- MAGIC Records in Delta Lake tables are stored as data in Parquet files.
-- MAGIC
-- MAGIC Transactions to Delta Lake tables are recorded in the **`_delta_log`**.
-- MAGIC
-- MAGIC We can peek inside the **`_delta_log`** to see more.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-dbcbd76a-c740-40be-8893-70e37bd5e0d2
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Each transaction results in a new JSON file being written to the Delta Lake transaction log. Here, we can see that there are 8 total transactions against this table (Delta Lake is 0 indexed).

-- COMMAND ----------

-- DBTITLE 0,--i18n-101dffc0-260a-4078-97db-cb1de8d705a8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Reasoning about Data Files
-- MAGIC
-- MAGIC We just saw a lot of data files for what is obviously a very small table.
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** allows us to see some other details about our Delta table, including the number of files.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-cb630727-afad-4dde-9d71-bcda9e579de9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Here we see that our table currently contains 4 data files in its present version. So what are all those other Parquet files doing in our table directory? 
-- MAGIC
-- MAGIC Rather than overwriting or immediately deleting files containing changed data, Delta Lake uses the transaction log to indicate whether or not files are valid in a current version of the table.
-- MAGIC
-- MAGIC Here, we'll look at the transaction log corresponding the **`MERGE`** statement above, where records were inserted, updated, and deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`dbfs:/mnt/dbacademy-users/marcin.jasinski@volvo.com/data-engineer-learning-path/database.db/students/_delta_log/00000000000000000007.json`"))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-3221b77b-6d57-4654-afc3-dcb9dfa62be8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC The **`add`** column contains a list of all the new files written to our table; the **`remove`** column indicates those files that no longer should be included in our table.
-- MAGIC
-- MAGIC When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc6dee2e-406c-48b2-9780-74408c93162d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Compacting Small Files and Indexing
-- MAGIC
-- MAGIC Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted.
-- MAGIC
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- DBTITLE 0,--i18n-5f412c12-88c7-4e43-bda2-60ec5c749b2a
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Given how small our data is, **`ZORDER`** does not provide any benefit, but we can see all of the metrics that result from this operation.

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad93f7e-4bb1-4051-8b9c-b685164e3b45
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Reviewing Delta Lake Transactions
-- MAGIC
-- MAGIC Because all changes to the Delta Lake table are stored in the transaction log, we can easily review the <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- DBTITLE 0,--i18n-ed297545-7997-4e75-8bf6-0c204a707956
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC As expected, **`OPTIMIZE`** created another version of our table, meaning that version 8 is our most current version.
-- MAGIC
-- MAGIC Remember all of those extra data files that had been marked as removed in our transaction log? These provide us with the ability to query previous versions of our table.
-- MAGIC
-- MAGIC These time travel queries can be performed by specifying either the integer version or a timestamp.
-- MAGIC
-- MAGIC **NOTE**: In most cases, you'll use a timestamp to recreate data at a time of interest. For our demo we'll use version, as this is deterministic (whereas you may be running this demo at any time in the future).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 0,--i18n-d1d03156-6d88-4d4c-ae8e-ddfe49d957d7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC What's important to note about time travel is that we're not recreating a previous state of the table by undoing transactions against our current version; rather, we're just querying all those data files that were indicated as valid as of the specified version.

-- COMMAND ----------

-- DBTITLE 0,--i18n-78cf75b0-0403-4aa5-98c7-e3aabbef5d67
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Rollback Versions
-- MAGIC
-- MAGIC Suppose you're typing up query to manually delete some records from a table and you accidentally execute this query in the following state.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-7f7936c3-3aa2-4782-8425-78e6e7634d79
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that when we see a **`-1`** for number of rows affected by a delete, this means an entire directory of data has been removed.
-- MAGIC
-- MAGIC Let's confirm this below.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-9d3908f4-e6bb-40a6-92dd-d7d12d28a032
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Deleting all the records in your table is probably not a desired outcome. Luckily, we can simply rollback this commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- DBTITLE 0,--i18n-902966c3-830a-44db-9e59-dec82b98a9c2
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that a **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> is recorded as a transaction; you won't be able to completely hide the fact that you accidentally deleted all the records in the table, but you will be able to undo the operation and bring your table back to a desired state.

-- COMMAND ----------

-- DBTITLE 0,--i18n-847452e6-2668-463b-afdf-52c1f512b8d3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Cleaning Up Stale Files
-- MAGIC
-- MAGIC Databricks will automatically clean up stale log files (> 30 days by default) in Delta Lake tables.
-- MAGIC Each time a checkpoint is written, Databricks automatically cleans up log entries older than this retention interval.
-- MAGIC
-- MAGIC While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive (and can lead to compliance issues if PII is present).
-- MAGIC
-- MAGIC If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation.
-- MAGIC
-- MAGIC Uncomment the following cell and execute it with a retention of **`0 HOURS`** to keep only the current version:

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3af389e-e93f-433c-8d47-b38f8ded5ecd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By default, **`VACUUM`** will prevent you from deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.  In our demos, you may see Databricks executing code that specifies a retention of **`0 HOURS`**. This is simply to demonstrate the feature and is not typically done in production.  
-- MAGIC
-- MAGIC In the following cell, we:
-- MAGIC 1. Turn off a check to prevent premature deletion of data files
-- MAGIC 1. Make sure that logging of **`VACUUM`** commands is enabled
-- MAGIC 1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c825ee6-e584-48a1-8d75-f616d7ed53ac
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By running **`VACUUM`** and deleting the 10 files above, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-6574e909-c7ee-4b0b-afb8-8bac83dacdd3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3437d5f0-c0e2-4486-8142-413a1849bc40
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
