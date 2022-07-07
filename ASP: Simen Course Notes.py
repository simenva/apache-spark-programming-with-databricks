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

# COMMAND ----------

#ASP 2.1
#Dataframe actions are methods that trigger computation, e.g. df.count()

#SparkSession is the single entry point to all DataFrame API functionality (automatically created in Databricks as a variable Spark)
createOrReplaceTempView #creates a temporary view based on the DataFrame. The lifetime of the temporary view is tied to the SparkSession that was used to create the DataFrame.

#Create a spark DataFrame
events_df = spark.table("events")

#Display DataFrame
events.df.display()
events_df.printScehma()

#Apply transformations
events_df_transformed = (events.df
                         .select(*)
                         .where("device = 'macOS'")
                         .sort("event_timestamp")
                        )

#Create same DataFrame using SQL
events_df = spark.sql("SELECT * from events WHERE (device = 'macOS') ORDER BY event_timestamp")
display(events_df)



# COMMAND ----------

#Readers and Writer

#Apache Parquet: Columnar storage format

spark.read.parquet(/mnt...)
(df.write
    .option(...))

spark.read.parquet("path/to/files")

Det går tregt å lese inn csv/json når man benytter "infer schema", og det går mye raskere dersom man definerer schema på forhånd. Kan bruke scala funksjon for å hente DDLSchema basert på en tilgjengelig fil, slik at man slipper å definere schema manuelt.

 WARNING: Do not use this trick in production
the inference of a schema can be REALLY slow as it
forces a full read of the source dataset to infer the schema



# COMMAND ----------

#Write dataframe to Files (output in Parquet)
users_output_dir = working_dir + "/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

#Alternatively
(users_df
 .write
 .parquet(users_output_dir, compression="snappy", mode="overwrite")
)

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

#Write events_df to a global table using the DataFrameWriter method saveAsTable
events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

#Write results to a Delta Table
events_output_path = working_dir + "/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)
#reference: https://spark.apache.org/docs/3.1.1/api/java/org/apache/spark/sql/DataFrameWriter.html
