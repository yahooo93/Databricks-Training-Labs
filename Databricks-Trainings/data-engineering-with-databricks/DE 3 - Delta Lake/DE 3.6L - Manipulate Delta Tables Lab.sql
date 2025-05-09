-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-65583202-79bf-45b7-8327-d4d5562c831d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Manipulating Delta Tables Lab
-- MAGIC
-- MAGIC This notebook provides a hands-on review of some of the more esoteric features Delta Lake brings to the data lakehouse.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Review table history
-- MAGIC - Query previous table versions and rollback a table to a specific version
-- MAGIC - Perform file compaction and Z-order indexing
-- MAGIC - Preview files marked for permanent deletion and commit these deletes

-- COMMAND ----------

-- DBTITLE 0,--i18n-065e2f94-2251-4701-b0b6-f4b86323dec8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Setup
-- MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-56940be8-afa9-49d8-8949-b4bcdb343f9d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Create History of Bean Collection
-- MAGIC
-- MAGIC The cell below includes various table operations, resulting in the following schema for the **`beans`** table:
-- MAGIC
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf6ff074-4166-4d51-92e5-67e7f2084c9b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Review the Table History
-- MAGIC
-- MAGIC Delta Lake's transaction log stores information about each transaction that modifies a table's contents or settings.
-- MAGIC
-- MAGIC Review the history of the **`beans`** table below.

-- COMMAND ----------

-- TODO
describe history beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fb56d746-8889-41c1-ba73-576282582534
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC If all the previous operations were completed as described you should see 7 versions of the table (**NOTE**: Delta Lake versioning starts with 0, so the max version number will be 6).
-- MAGIC
-- MAGIC The operations should be as follows:
-- MAGIC
-- MAGIC | version | operation |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC
-- MAGIC The **`operationsParameters`** column will let you review predicates used for updates, deletes, and merges. The **`operationMetrics`** column indicates how many rows and files are added in each operation.
-- MAGIC
-- MAGIC Spend some time reviewing the Delta Lake history to understand which table version matches with a given transaction.
-- MAGIC
-- MAGIC **NOTE**: The **`version`** column designates the state of a table once a given transaction completes. The **`readVersion`** column indicates the version of the table an operation executed against. In this simple demo (with no concurrent transactions), this relationship should always increment by 1.

-- COMMAND ----------

-- DBTITLE 0,--i18n-00d8e251-9c9e-4be3-b8e7-6e38b07fac55
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Specific Version
-- MAGIC
-- MAGIC After reviewing the table history, you decide you want to view the state of your table after your very first data was inserted.
-- MAGIC
-- MAGIC Run the query below to see this.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 0,--i18n-90e3c115-6bed-4b83-bb37-dd45fb92aec5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC And now review the current state of your data.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-f073a6d9-3aca-41a0-9452-a278fb87fa8c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC You want to review the weights of your beans before you deleted any records.
-- MAGIC
-- MAGIC Fill in the statement below to register a temporary view of the version just before data was deleted, then run the following cell to query the view.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
SELECT * FROM beans VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-bad13c31-d91f-454e-a14e-888d255dc8a4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to check that you have captured the correct version.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- DBTITLE 0,--i18n-8450d1ef-c49b-4c67-9390-3e0550c9efbc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Restore a Previous Version
-- MAGIC
-- MAGIC Apparently there was a misunderstanding; the beans your friend gave you that you merged into your collection were not intended for you to keep.
-- MAGIC
-- MAGIC Revert your table to the version before this **`MERGE`** statement completed.

-- COMMAND ----------

-- TODO
RESTORE TABLE beans TO VERSION AS OF 5

-- COMMAND ----------

-- DBTITLE 0,--i18n-405edc91-49e8-412b-99e7-96cc60aab32d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Review the history of your table. Make note of the fact that restoring to a previous version adds another table version.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- DBTITLE 0,--i18n-d430fe1c-32f1-44c0-907a-62ef8a5ca07b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## File Compaction
-- MAGIC Looking at the transaction metrics during your reversion, you are surprised you have some many files for such a small collection of data.
-- MAGIC
-- MAGIC While indexing on a table of this size is unlikely to improve performance, you decide to add a Z-order index on the **`name`** field in anticipation of your bean collection growing exponentially over time.
-- MAGIC
-- MAGIC Use the cell below to perform file compaction and Z-order indexing.

-- COMMAND ----------

-- TODO
OPTIMIZE beans
ZORDER BY (name)

-- COMMAND ----------

-- DBTITLE 0,--i18n-8ef4ffb6-c958-4798-b564-fd2e65d4fa0e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Your data should have been compacted to a single file; confirm this manually by running the following cell.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-8a63081c-1423-43f2-9608-fe846a4a58bb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to check that you've successfully optimized and indexed your table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6432b28c-18c1-4402-864c-ea40abca50e1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Cleaning Up Stale Data Files
-- MAGIC
-- MAGIC You know that while all your data now resides in 1 data file, the data files from previous versions of your table are still being stored alongside this. You wish to remove these files and remove access to previous versions of the table by running **`VACUUM`** on the table.
-- MAGIC
-- MAGIC Executing **`VACUUM`** performs garbage cleanup on the table directory. By default, a retention threshold of 7 days will be enforced.
-- MAGIC
-- MAGIC The cell below modifies some Spark configurations. The first command overrides the retention threshold check to allow us to demonstrate permanent removal of data. 
-- MAGIC
-- MAGIC **NOTE**: Vacuuming a production table with a short retention can lead to data corruption and/or failure of long-running queries. This is for demonstration purposes only and extreme caution should be used when disabling this setting.
-- MAGIC
-- MAGIC The second command sets **`spark.databricks.delta.vacuum.logging.enabled`** to **`true`** to ensure that the **`VACUUM`** operation is recorded in the transaction log.
-- MAGIC
-- MAGIC **NOTE**: Because of slight differences in storage protocols on various clouds, logging **`VACUUM`** commands is not on by default for some clouds as of DBR 9.1.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- DBTITLE 0,--i18n-04f27ab4-7848-4418-ac79-c339f9843b23
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Before permanently deleting data files, review them manually using the **`DRY RUN`** option.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb9ce589-09ae-47b8-b6a6-4ab8e4dc70e7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC All data files not in the current version of the table will be shown in the preview above.
-- MAGIC
-- MAGIC Run the command again without **`DRY RUN`** to permanently delete these files.
-- MAGIC
-- MAGIC **NOTE**: All previous versions of the table will no longer be accessible.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-1630420a-94f5-43eb-b37c-ccbb46c9ba40
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Because **`VACUUM`** can be such a destructive act for important datasets, it's always a good idea to turn the retention duration check back on. Run the cell below to reactive this setting.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- DBTITLE 0,--i18n-8d72840f-49f1-4983-92e4-73845aa98086
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that the table history will indicate the user that completed the **`VACUUM`** operation, the number of files deleted, and log that the retention check was disabled during this operation.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-875b39be-103c-4c70-8a2b-43eaa4a513ee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Query your table again to confirm you still have access to the current version.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdb81194-2e3e-4a00-bfb6-97e822ae9ec3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Because Delta Cache stores copies of files queried in the current session on storage volumes deployed to your currently active cluster, you may still be able to temporarily access previous table versions (though systems should **not** be designed to expect this behavior). 
-- MAGIC
-- MAGIC Restarting the cluster will ensure that these cached data files are permanently purged.
-- MAGIC
-- MAGIC You can see an example of this by uncommenting and running the following cell that may, or may not, fail
-- MAGIC (depending on the state of the cache).

-- COMMAND ----------

--SELECT * FROM beans@v1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a5cfd876-c53a-4d60-96e2-cdbc2b00c19f
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands
-- MAGIC * Reviewing table metadata including table history
-- MAGIC * Leverage Delta Lake versioning for snapshot queries and rollbacks
-- MAGIC * Compacting small files and indexing tables
-- MAGIC * Using **`VACUUM`** to review files marked for deletion and committing these deletes

-- COMMAND ----------

-- DBTITLE 0,--i18n-b541b92b-03a9-4f3c-b41c-fdb0ce4f2271
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
