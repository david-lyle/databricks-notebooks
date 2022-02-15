# Databricks notebook source
# This creates the "team_name" field displayed at the top of the notebook.

dbutils.widgets.text("team_name", "Enter your team's name");

# COMMAND ----------

# Note that we have factored out the setup processing into a different notebook, which we call here.
# As a flight school student, you will probably want to look at the setup notebook.  
# Even though you'll want to look at it, we separated it out in order to demonstrate a best practice... 
# ... you can use this technique to keep your demos shorter, and avoid boring your audience with housekeeping.
# In addition, you can save demo time by running this initial setup command before you begin your demo.

# This cell should run in a few minutes or less

team_name = dbutils.widgets.get("team_name")

setup_responses = dbutils.notebook.run("./includes/flight_school_assignment_2_setup", 0, {"team_name": team_name}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

# COMMAND ----------

slide_id = '1OlHTofpGf-eM3RDv1riqQYJtMk7StxEcm-5CcdSxqxc'
slide_number = 'id.p'

displayHTML(f'''
<iframe
  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
  frameborder="0"
  width="65%"
  height="600"
></iframe>
''')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## What is a Stream? <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/spark-streaming-logo.png?raw=true" width=200/>
# MAGIC 
# MAGIC The "traditional" way to process data is in batches.  In a __batch__, a collection of data is complete and "at rest" *before* we access it.
# MAGIC 
# MAGIC In contrast, a data __stream__ is a __continuous flow of data__.  When we access a stream, we expect that new data will appear *after* we open the stream.  The data may arrive in regular or irregular intervals.  Common __examples__ of streams include:
# MAGIC 
# MAGIC - Telemetry from devices 
# MAGIC - Log files from applications 
# MAGIC - Transactions created by users
# MAGIC - Real time weather data
# MAGIC 
# MAGIC Common stream __technologies__ include:
# MAGIC 
# MAGIC - Sockets
# MAGIC - Java Message Service (JMS) applications, including RabbitMQ, ActiveMQ, IBM MQ, etc.
# MAGIC - Apache Kafka
# MAGIC - Azure Event Hub
# MAGIC 
# MAGIC As we'll see in this workshop, __Delta Lake__ is also a highly effective solution for streaming use cases.  
# MAGIC 
# MAGIC The diagram below outlines the approach Spark takes to deal with streams.  We're using the newer Spark streaming technology, which is known as __Structured Streaming__.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/spark-streaming-basic-concept.png?raw=true" />
# MAGIC 
# MAGIC As the diagram above illustrates, Spark considers a stream to be an "unbounded" or "infinite" table.  This is a brilliant concept because it allows us to use SQL against a stream.  However, as we shall see, there are some limitations.  
# MAGIC 
# MAGIC Keep in mind that a stream is constantly __*appending*__ data.  In streaming, we never update or delete an existing piece of data.  
# MAGIC 
# MAGIC #### OK, let's get started!

# COMMAND ----------

# Here is a bit of Spark housekeeping.  When we aggregate our stream later in this demo, Spark will need to shuffle data.
# For our small demo data size, we'll get much better performance by reducing the number of shuffle partitions.
# Default is 200.  Most of them won't be used, but each will have to wait for an available core, then start up and shut down.
# Let's run this cell to reduce the number of shuffle partitions.

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# The setup you ran at the beginning of this Workshop module created a database for you.
# However, it doesn't have any tables in it yet.

# Let's set this database to be the default, so we don't have to name it in every operation.
# Note that this cell uses Python, but the spark.sql() method enables us to do our work with standard SQL.  The "USE" command specifies the default database.

spark.sql(f'USE {database_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a stream
# MAGIC 
# MAGIC A Delta Lake table can be a streaming source, sink, or both.
# MAGIC 
# MAGIC We're going to use a __Delta Lake table__, `readings_stream_source`, as our __stream source__.  It's just an ordinary Delta Lake table, but when we run "readStream" against it below, it will become a streaming source.

# COMMAND ----------

# Create a stream from the landing table
# To read data as a stream, we simply call readStream instead of read.
# Complete this call... use "delta" as the format, and read table "readings_stream_source"
readings_stream = spark \
                   .readStream \
                   .format("delta") \
                   .table("readings_stream_source")

# Register the stream as a temporary view so we can run SQL on it
readings_stream.createOrReplaceTempView("readings_streaming")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consume the stream in APPEND mode
# MAGIC 
# MAGIC First we'll read all incoming records at the individual record level as they come in on the stream.
# MAGIC 
# MAGIC Here we'll just read the records, but keep in mind that we could perform arbitrary processing on them, including writing them out to another table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read detail-level records from our stream
# MAGIC -- Remember, you will have to cancel this cell when you are done.
# MAGIC -- Otherwise, your cluster will run forever.
# MAGIC 
# MAGIC -- NOTE: the stream will not see any activity until we begin writing records in a cell below.
# MAGIC select * from readings_streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS readings_stream_sink;
# MAGIC 
# MAGIC CREATE TABLE readings_stream_sink
# MAGIC   ( id INTEGER, 
# MAGIC     reading_time TIMESTAMP, 
# MAGIC     device_type STRING,
# MAGIC     device_id STRING,
# MAGIC     device_operational_status STRING,
# MAGIC     reading_1 DOUBLE,
# MAGIC     reading_2 DOUBLE,
# MAGIC     reading_3 DOUBLE )  
# MAGIC --   TBLPROPERTIES
# MAGIC --   (delta.targetFileSize = 104857, delta.autoOptimize.optimizeWrite = false, delta.autoOptimize.autoCompact = true, spark.databricks.delta.autoCompact.maxFileSize = 104857)

# COMMAND ----------

#Write the stream to a different delta table with optimize every 10 batches
from pyspark.sql.functions import *
from pyspark.sql import *


dbutils.fs.rm(f"/FileStore/flight/{dbfs_data_path}sink_checkpoint", True)

spark.conf.set(
  "spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled",
  "true"
)

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)
spark.conf.set("spark.databricks.delta.checkpoint.async.enabled", "false")

# def optimizeEvery10th(df, batchId):
#   df.persist()
#   #if batchId % 10 == 0:
#     #spark.sql("optimize readings_stream_sink")
#   df.write.format("delta").mode("append").saveAsTable("readings_stream_sink")
#   df.unpersist()
  
# writerQuery = readings_stream.writeStream.format("delta") \
#   .foreachBatch(optimizeEvery10th) \
#   .option("checkpointLocation", f"/FileStore/flight/{dbfs_data_path}sink_checkpoint") \
#   .trigger(processingTime='10 seconds') \
#   .outputMode("update") \
#   .start()

writerQuery = readings_stream.writeStream.format("delta") \
  .option("checkpointLocation", f"/FileStore/flight/{dbfs_data_path}sink_checkpoint") \
  .outputMode("append") \
  .table('readings_stream_sink')

# COMMAND ----------

spark.conf.get("spark.databricks.delta.checkpoint.async.enabled")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from readings_stream_sink

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", 'false')

# COMMAND ----------

# MAGIC %sql 
# MAGIC set spark.databricks.delta.checkpoint.async.enabled 

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -alSh /dbfs/user/hive/warehouse/flight_school_fsteam2.db/readings_stream_sink/_delta_log/
# MAGIC #echo
# MAGIC #ls -1 /dbfs/user/hive/warehouse/flight_school_fsteam2.db/readings_stream_sink | wc -l

# COMMAND ----------

print(writerQuery.lastProgress)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended readings_stream_sink 

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum readings_stream_sink retain 0 hours dry run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consuming streams in aggregate: Initiate 3 stream listeners
# MAGIC 
# MAGIC Above we started an example of detail-level append streaming.  Now we'll concentrate on ways to __aggregate__ streams into useful summaries.
# MAGIC 
# MAGIC Take special note of the difference between __full aggregation__ and __windowed aggregation__, which we use in the second and third listeners.  The diagram below shows how windowed aggregation works:
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/databricks-demo-delta-streaming/blob/master/images/windowed_aggregation.png?raw=true" />
# MAGIC 
# MAGIC As the diagram illustrates, windowed aggregation uses only a subset of the data.  This subset is typically chosen based on a timestamp range.  Note that the timestamp is from the data itself, not from the current time of our code in this notebook.  In the diagram above, we are using a timestamp column to select data within 10-minute ranges.
# MAGIC 
# MAGIC In addition, we can __slide__ the window.  In the diagram above, we are sliding the 10-minute window every 5 minutes.  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- full aggregation
# MAGIC 
# MAGIC -- This is a streaming query that aggregates the entire stream "from the beginning of time."
# MAGIC 
# MAGIC -- Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.
# MAGIC 
# MAGIC -- Can you create some interesting graphical views on this data?
# MAGIC 
# MAGIC SELECT 
# MAGIC   device_type, 
# MAGIC   avg(reading_1) as reading_1,
# MAGIC   count(reading_1) as count_reading_1
# MAGIC FROM readings_streaming
# MAGIC GROUP BY device_type 
# MAGIC ORDER BY device_type ASC
# MAGIC 
# MAGIC -- Before you move on from this cell, make sure the "Stream initializing" message below goes away.
# MAGIC 
# MAGIC -- REMEMBER... you will have to CANCEL the streaming query.
# MAGIC -- Otherwise, your cluster will run indefinitely.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- windowed aggregation
# MAGIC 
# MAGIC -- This is also a streaming query that reads the readings_streaming stream.
# MAGIC -- It also calculates the average reading_1 for each device type, and also the COUNT for each device_type.
# MAGIC -- However, note that this average and count is NOT calculated for the entire stream, since "the beginning of time."
# MAGIC -- Instead, this query serves a "dashboard" use case, because it calculates the average reading over a 2-minute window of time.
# MAGIC -- Furthermore, this 2-minute window "slides" by a minute once per minute.
# MAGIC 
# MAGIC -- Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.
# MAGIC 
# MAGIC SELECT 
# MAGIC   window, -- this is a reserved word that lets us define windows
# MAGIC   device_type, 
# MAGIC   avg(reading_1) AS reading_1, 
# MAGIC   count(reading_1) AS count_reading_1 
# MAGIC FROM readings_streaming 
# MAGIC GROUP BY 
# MAGIC   window(reading_time, '2 minutes', '1 minute'), -- NOTE that the window uses a data column in the record.  It is NOT the current time in our code.
# MAGIC   device_type 
# MAGIC ORDER BY 
# MAGIC   window DESC, 
# MAGIC   device_type ASC 
# MAGIC LIMIT 4 -- this lets us see the last two window aggregations for device_type
# MAGIC 
# MAGIC -- Before you move on from this cell, make sure the "Stream initializing" message below goes away.
# MAGIC 
# MAGIC -- REMEMBER... you will have to CANCEL the streaming query.
# MAGIC -- Otherwise, your cluster will run indefinitely.

# COMMAND ----------

# windowed aggregation WITH STREAMING OUTPUT

# This is the same aggregate query we ran above (except we remove the "LIMIT 4", but here we also WRITE the aggregated stream to a Delta Lake table.
# We could have done all this in the original query, of course, but we wanted to introduce the concepts one at a time.

# This is useful because now another independent application can read the Delta Lake pre-aggregated table at will to 
# easily produce a dashboard or other useful output.

# Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.

out_stream = spark.sql("""
                          SELECT 
                            window, -- this is a reserved word that lets us define windows
                              device_type, 
                              avg(reading_1) AS reading_1, 
                              count(reading_1) AS count_reading_1 
                          FROM readings_streaming 
                            GROUP BY 
                              window(reading_time, '2 minutes', '1 minute'), -- NOTE that the window uses a data column in the record.  It is NOT the current time in our code.
                              device_type 
                            ORDER BY 
                              window DESC, 
                              device_type ASC
                        """)

# Here we create a new output stream, which is an aggregated version of the data from our original stream.
# Note that we use .outputMode("complete")...
# Spark Structured Streaming has three output modes:
#  - append - this is used when we want to write detail-level records.  You cannot query an entire table of detail records, 
#             you can only see the latest data in the stream.
# - complete - however, if you query a stream with an aggregate query, Spark will retain and update the entire aggregate.
#              In this mode, we will continue to output new versions of the entire aggregation result set.
# - update - this is similar to complete, and is also used for aggregations.  The difference is that "update" will only
#            write the *changed* rows of an aggregate result set.

# First delete the checkpoint file, just in case it exists from a prior run
dbutils.fs.rm(f"/FileStore/flight/{dbfs_data_path}streaming_ckpnt", recurse=True)

out_stream.writeStream.format("delta") \
  .outputMode("complete") \
  .option("checkpointLocation", f"/FileStore/flight/{dbfs_data_path}streaming_ckpnt") \
  .table("readings_agg") \
        
# Before you move on from this cell, make sure the "Stream initializing" message below goes away.

# REMEMBER... you will have to CANCEL the streaming query.
# Otherwise, your cluster will run indefinitely.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine the new sink we created
# MAGIC 
# MAGIC Let's take a look at the new Delta Lake table we created above using writeStream().
# MAGIC 
# MAGIC This would be a great data source for a separate dashboard application.  The application could simply query the table at will, and format the results.  It wouldn't have to worry at all about streaming or aggregation, since the information has already been prepared and formatted.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now imagine that this is our independent dashboard-building program.
# MAGIC -- It can read the pre-aggregated data on Delta Lake at will, and simply display the data.
# MAGIC select window.start, window.end, device_type, reading_1, count_reading_1 from readings_agg 
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine the 4 streaming queries we started above.  
# MAGIC 
# MAGIC Take a look at the data generated by our four streaming readers.  Notice the difference between detail-level, full aggregation, and windowed aggregation.
# MAGIC 
# MAGIC Also click <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/data_icon.png?raw=true" /> in the left sidebar and note that our stream writer has created a new table, readings_agg.

# COMMAND ----------

slide_id = '1OlHTofpGf-eM3RDv1riqQYJtMk7StxEcm-5CcdSxqxc'
slide_number = 'id.g1015e5f89bd_0_0'

displayHTML(f'''
<iframe
  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
  frameborder="0"
  width="75%"
  height="600"
></iframe>
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists sample_stream;
# MAGIC 
# MAGIC CREATE TABLE sample_stream
# MAGIC   ( id INTEGER, 
# MAGIC     reading_time TIMESTAMP, 
# MAGIC     device_type STRING,
# MAGIC     device_id STRING,
# MAGIC     device_operational_status STRING,
# MAGIC     reading_1 DOUBLE,
# MAGIC     reading_2 DOUBLE,
# MAGIC     reading_3 DOUBLE )
# MAGIC   USING DELTA 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

def take10Percent(df, epochId): 
  take10Percent.counter += 1
  if take10Percent.counter % 10 == 0:
    take10Percent.counter = 0
    df.write \
      .format("delta") \
      .mode('append') \
      .insertInto("sample_stream")

take10Percent.counter = 0

readings_stream.writeStream \
  .format("delta") \
  .outputMode("append") \
  .foreachBatch(take10Percent) \
  .start()





# COMMAND ----------

samples_stream = spark \
                   .readStream \
                   .format("delta") \
                   .table("sample_stream")

samples_stream.createOrReplaceTempView("sample_streaming")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sample_streaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is a Watermark?
# MAGIC Watermarking is a useful method which helps a Stream Processing Engine to deal with lateness. Basically, a watermark is a threshold to specify how long the system waits for late events. If an arriving event lies within the watermark, it gets used to update a query. Otherwise, if it’s older than the watermark, it will be dropped and not further processed by the Streaming Engine.
# MAGIC 
# MAGIC <img src = "https://drive.google.com/uc?id=1HnWaZMecG-AYOQRFk0xsFz8I8OnN5UgB"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. let's check how many records we have in our source
# MAGIC 2. then create duplicated data for some id's

# COMMAND ----------

spark.sql("SELECT * FROM current_readings_labeled").count()

## am duplicating the data in dataframe dfDup
df1 = spark.sql("SELECT * FROM current_readings_labeled limit 10")
dfDup = spark.sql("select * from current_readings_labeled limit 100").union(df1)
dfDup.count()

## now the id's from 1-10 are duplicated

dfDup.filter("id In (1,2,3)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC creating a stream out of the duplicated data so that we can show how de-duplicating works in the streams

# COMMAND ----------

dfDup.write.mode("overwrite").saveAsTable("duplicatedLogs")

duplicatedStream = spark \
                   .readStream \
                   .format("delta") \
                    .option("rowsPerSecond",1)\
                   .table("duplicatedLogs")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Just as other stateful processing APIs in Structured Streaming are bounded by declaring watermarking for late data semantics, so is dropping duplicates. Without watermarking, the maintained state can grow infinitely over the course of your stream.
# MAGIC 
# MAGIC The API to instruct Structured Streaming to drop duplicates is as simple as all other APIs we have shown so far in our blogs and documentation. Using the API, you can declare arbitrarily columns on which to drop duplicates—for example, user_id and timestamp. An entry with same timestamp and user_id is marked as duplicate and dropped, but the same entry with two different timestamps is not.

# COMMAND ----------

## removing duplicates using Watermark
duplicatedStream \
  .withWatermark("reading_time","10 seconds")\
  .dropDuplicates(["id","reading_time"])\
  .groupBy("id")\
  .sum("reading_1")\
  .writeStream \
  .queryName("deduplicated")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. checking if de-duplication happens or not

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deduplicated where id in (1,2,3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.

# COMMAND ----------

slide_id = '1OlHTofpGf-eM3RDv1riqQYJtMk7StxEcm-5CcdSxqxc'
slide_number = 'id.p'

displayHTML(f'''
<iframe
  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
  frameborder="0"
  width="65%"
  height="600"
></iframe>
''')
