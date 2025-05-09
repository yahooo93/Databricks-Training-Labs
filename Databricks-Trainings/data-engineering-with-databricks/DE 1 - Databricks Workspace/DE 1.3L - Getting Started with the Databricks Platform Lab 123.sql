-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-4212c527-b1c1-4cce-b629-b5ffb5c57d68
-- MAGIC %md
-- MAGIC
-- MAGIC # Getting Started with the Databricks Platform
-- MAGIC
-- MAGIC This notebook provides a hands-on review of some of the basic functionality of the Databricks Data Science and Engineering Workspace.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Rename a notebook and change the default language
-- MAGIC - Attach a cluster
-- MAGIC - Use the **`%run`** magic command
-- MAGIC - Run Python and SQL cells
-- MAGIC - Create a Markdown cell

-- COMMAND ----------

-- DBTITLE 0,--i18n-eb166a05-9a22-4a74-b54e-3f9e5779f342
-- MAGIC %md
-- MAGIC
-- MAGIC # Renaming a Notebook
-- MAGIC
-- MAGIC Changing the name of a notebook is easy. Click on the name at the top of this page, then make changes to the name. To make it easier to navigate back to this notebook later in case you need to, append a short test string to the end of the existing name.

-- COMMAND ----------

-- DBTITLE 0,--i18n-a975b60c-9871-4736-9b4f-194577d730f0
-- MAGIC %md
-- MAGIC
-- MAGIC # Attaching a cluster
-- MAGIC
-- MAGIC Executing cells in a notebook requires computing resources, which is provided by clusters. The first time you execute a cell in a notebook, you will be prompted to attach to a cluster if one is not already attached.
-- MAGIC
-- MAGIC Attach a cluster to this notebook now by clicking the dropdown near the top-right corner of this page. Select the cluster you created previously. This will clear the execution state of the notebook and connect the notebook to the selected cluster.
-- MAGIC
-- MAGIC Note that the dropdown menu provides the option of starting or restarting the cluster as needed. You can also detach and re-attach to a cluster in a single operation. This is useful for clearing the execution state when needed.

-- COMMAND ----------

-- DBTITLE 0,--i18n-4cd4b089-e782-4e81-9b88-5c0abd02d03f
-- MAGIC %md
-- MAGIC
-- MAGIC # Using %run
-- MAGIC
-- MAGIC Complex projects of any type can benefit from the ability to break them down into simpler, reusable components.
-- MAGIC
-- MAGIC In the context of Databricks notebooks, this facility is provided through the **`%run`** magic command.
-- MAGIC
-- MAGIC When used this way, variables, functions and code blocks become part of the current programming context.
-- MAGIC
-- MAGIC Consider this example:
-- MAGIC
-- MAGIC **`Notebook_A`** has four commands:
-- MAGIC   1. **`name = "John"`**
-- MAGIC   2. **`print(f"Hello {name}")`**
-- MAGIC   3. **`%run ./Notebook_B`**
-- MAGIC   4. **`print(f"Welcome back {full_name}`**
-- MAGIC
-- MAGIC **`Notebook_B`** has only one commands:
-- MAGIC   1. **`full_name = f"{name} Doe"`**
-- MAGIC
-- MAGIC If we run **`Notebook_B`** it will fail to execute because the variable **`name`** is not defined in **`Notebook_B`**
-- MAGIC
-- MAGIC Likewise, one might think that **`Notebook_A`** would fail becase it uses the variable **`full_name`** which is likewise not defined in **`Notebook_A`**, but it doesn't!
-- MAGIC
-- MAGIC What actually happens is that the two notebooks are merged together as we see below and **then** executed:
-- MAGIC 1. **`name = "John"`**
-- MAGIC 2. **`print(f"Hello {name}")`**
-- MAGIC 3. **`full_name = f"{name} Doe"`**
-- MAGIC 4. **`print(f"Welcome back {full_name}")`**
-- MAGIC
-- MAGIC And thus providing the expected behavior:
-- MAGIC * **`Hello John`**
-- MAGIC * **`Welcome back John Doe`**

-- COMMAND ----------

-- DBTITLE 0,--i18n-40ca42ab-4275-4d92-b151-995429e54486
-- MAGIC %md
-- MAGIC
-- MAGIC The folder that contains this notebook contains a subfolder named **`ExampleSetupFolder`**, which in turn contains a notebook called **`example-setup`**. 
-- MAGIC
-- MAGIC This simple notebook declares the variable **`my_name`**, sets it to **`None`**, and then creates a DataFrame called **`example_df`**. 
-- MAGIC
-- MAGIC Open the **`example-setup`** notebook and modify it so that **`my_name`** is not **`None`** but rather your name (or anyone's name) enclosed in quotes, and so that the following two cells execute without throwing an **`AssertionError`**.
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> You will see additional references **`_utility-methods`** and **`DBAcademyHelper`** which are used to this configure  courseware and should be ignored for this exercise.

-- COMMAND ----------

-- MAGIC %run ./ExampleSetupFolder/example-setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert my_name is not None, "Name is still None"
-- MAGIC print(my_name)

-- COMMAND ----------

-- DBTITLE 0,--i18n-e5ef8dff-bfa6-4f9e-8ad3-d5ef322b978d
-- MAGIC %md
-- MAGIC
-- MAGIC ## Run a Python cell
-- MAGIC
-- MAGIC Run the following cell to verify that the **`example-setup`** notebook was executed by displaying the **`example_df`** DataFrame. This table consists of 16 rows of increasing values.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(example_df)

-- COMMAND ----------

-- DBTITLE 0,--i18n-6cb46bcc-9797-4782-931c-a7b8350146b2
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Change Language
-- MAGIC
-- MAGIC Notice that the default language for this notebook is set to Python. Change this by clicking the **Python** button to the right of the notebook name. Change the default language to SQL.
-- MAGIC
-- MAGIC Notice that the Python cells are automatically prepended with a <strong><code>&#37;python</code></strong> magic command to maintain validity of those cells. Notice that this operation also clears the execution state.

-- COMMAND ----------

-- DBTITLE 0,--i18n-478faa69-6814-4725-803b-3414a1a803ae
-- MAGIC %md
-- MAGIC
-- MAGIC # Create a Markdown Cell
-- MAGIC
-- MAGIC Add a new cell below this one. Populate with some Markdown that includes at least the following elements:
-- MAGIC * A header
-- MAGIC * Bullet points
-- MAGIC * A link (using your choice of HTML or Markdown conventions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Test markdown
-- MAGIC * this is test
-- MAGIC * this is also test
-- MAGIC * this is a test too
-- MAGIC
-- MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target= "_blank">Markdown - Wikipedia</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-55b2a6c6-2fc6-4c57-8d6d-94bba244d86e
-- MAGIC %md
-- MAGIC
-- MAGIC ## Run a SQL cell
-- MAGIC
-- MAGIC Run the following cell to query a Delta table using SQL. This executes a simple query against a table is backed by a Databricks-provided example dataset included in all DBFS installations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9993ed50-d8cf-4f37-bc76-6b18789447d6
-- MAGIC %md
-- MAGIC
-- MAGIC Execute the following cell to view the underlying files backing this table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-c31a3318-a114-46e8-a744-18e8f8aa071e
-- MAGIC %md
-- MAGIC
-- MAGIC # Clearing notebook state
-- MAGIC
-- MAGIC Sometimes it is useful to clear all variables defined in the notebook and start from the begining.  This can be useful when you want to test cells in isolation, or you simply want to reset the execution state.
-- MAGIC
-- MAGIC Visit the **Run** menu and select the **Clear state and outputs**.
-- MAGIC
-- MAGIC Now try running the cell below and notice the variables defined earlier are no longer defined, until you rerun the earlier cells above.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(my_name)

-- COMMAND ----------

-- DBTITLE 0,--i18n-1e11bea0-7be9-4df7-be4e-b525c625dfee
-- MAGIC %md
-- MAGIC
-- MAGIC # Review Changes
-- MAGIC
-- MAGIC Assuming you have imported this material into your workspace using a Databricks Repo, open the Repo dialog by clicking the **`published`** branch button at the top-left corner of this page. You should see three changes:
-- MAGIC 1. **Removed** with the old notebook name
-- MAGIC 1. **Added** with the new notebook name
-- MAGIC 1. **Modified** for creating a markdown cell above
-- MAGIC
-- MAGIC Use the dialog to revert the changes and restore this notebook to its original state.

-- COMMAND ----------

-- DBTITLE 0,--i18n-9947d429-2c10-4047-811f-3f5128527c6d
-- MAGIC %md
-- MAGIC
-- MAGIC ## Wrapping Up
-- MAGIC
-- MAGIC By completing this lab, you should now feel comfortable manipulating notebooks, creating new cells, and running notebooks within notebooks.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
