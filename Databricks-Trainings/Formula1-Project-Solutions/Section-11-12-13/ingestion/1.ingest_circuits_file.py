# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl0912/raw"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/formula1dl0912/raw"

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.csv("/mnt/formula1dl0912/raw/circuits.csv")

# circuits_df = spark.read \
# .csv("/mnt/formula1dl0912/raw/circuits.csv")

# COMMAND ----------

circuits_df.show(vertical=True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show() ###Helps to identify if schema is correctly assigned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("/mnt/formula1dl0912/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

##It is much better to use the schema api below rather than inferSchema() as inferSchema goes through a whole file each time it runs, requiring 2 spark jobs to be run each time which is not efficient.

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/formula1dl0912/raw/circuits.csv")

# COMMAND ----------

##In printschema all are nullable despite the fact we set it to nullable = False - this is standard behavior, to be checked 
circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

#This way enables to select columns - and that's it
circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

##The below ways enable not only select, but also apply some column-based functions (like rename?)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country.alias("race_country"),circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df['circuitId'],circuits_df['circuitRef'],circuits_df['name'],circuits_df['location'],circuits_df['country'].alias("race_country"),circuits_df['lat'],circuits_df['lng'],circuits_df['alt'])

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumnRenamed("race_country", "country")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
# .withColumn("env",lit("Production")) #---> this funcion enables to add some literal value to a dataframe as column object

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dl0912/processed/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl0912/processed/circuits"))

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl0912/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl0912/processed/circuits

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


