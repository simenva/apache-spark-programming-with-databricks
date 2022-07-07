# Databricks notebook source
# MAGIC %md
# MAGIC Spark benefits
# MAGIC - Fast (100times faster than hadoop)
# MAGIC - Easy to use APIs
# MAGIC - Unified (unified API and engine for SQL queries, streaming data and machine learning)
# MAGIC 
# MAGIC Core models:
# MAGIC - Spark SQL + DataFrames
# MAGIC - MLLib : scalable ML library built on Spark (up to 100x faster than MapReduce)
# MAGIC - Spark Core API: R, SQL, Python, Scala, Java
# MAGIC 
# MAGIC Spark Execution:
# MAGIC - Spark distributes the work on several machines (parallelism)
# MAGIC - each parallell is referred to as job
# MAGIC - each job consists of several stages
# MAGIC - each stage consist of several tasks
# MAGIC 
# MAGIC Spark Cluster:
# MAGIC - Driver is the machine in which the application runs
# MAGIC - Maintains information about the app
# MAGIC - Responds to the users program
# MAGIC - organizing and scheduling work across the executors
# MAGIC - worker node host the executor-process
# MAGIC - each executor holds a chunk of the data to be processed. Each chunck is referred to as a partition (a collection of rows on one machine)
# MAGIC - Core is also known as threads or slots

# COMMAND ----------

# MAGIC %md
# MAGIC Case Study: BedBricks

# COMMAND ----------

#Introduction
%fs ls # access the databricks virtual file system, like it was a local system
#fs is short for the dbutils.fs
%fs mounts
#liste alle filer i en tabell
%fs ls /databricks-datasets
#alternativ som gir samme tabell
files = dbutils.fs.ls("/databricks-datasets")
display(files)

# COMMAND ----------

spark.sql(f"SET c.events_path = {events_path}") # Declares the python variable as a variable in the spark context which SQL commands can acce
%sql
CREATE TABLE IF NOT EXISTS events
USING DELTA
OPTIONS (path = "${c.events_path}");

# COMMAND ----------

#Create inputs
dbutils.widgets.text("name", "Brickster", "Name") # creates input as text stored in variable name with default Brickster


# COMMAND ----------

name = dbutils.widgets.get("name") # access the current value with the get.function
print(name)
