# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC --CREATE database dlyle;
# MAGIC create or replace table dlyle.queryprogress (
# MAGIC batchId bigint,
# MAGIC durationMs struct<addBatch:bigint,getBatch:bigint,latestOffset:bigint,queryPlanning:bigint,triggerExecution:bigint,walCommit:bigint>,
# MAGIC id string,
# MAGIC inputRowsPerSecond double,
# MAGIC name string,
# MAGIC numInputRows bigint,
# MAGIC processedRowsPerSecond double,
# MAGIC runId string,
# MAGIC sink struct<description:string,numOutputRows:bigint>,
# MAGIC sources array<struct<description:string,endOffset:bigint,inputRowsPerSecond:double,latestOffset:bigint,numInputRows:bigint,processedRowsPerSecond:double,startOffset:bigint>>,
# MAGIC stateOperators array<string>,
# MAGIC timestamp string,
# MAGIC progressDate DATE GENERATED ALWAYS AS (TO_DATE(timestamp))
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (progressDate)
# MAGIC TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.http.client.methods.HttpPut
# MAGIC import org.apache.http.impl.client.HttpClientBuilder
# MAGIC 
# MAGIC def wipePushgateway(url: String): Int = {
# MAGIC   //Send PUT request to the wipe api on the pushgateway
# MAGIC   val client = HttpClientBuilder.create().build()
# MAGIC   val putUrl = url + "/api/v1/admin/wipe"
# MAGIC   val put = new HttpPut(putUrl)
# MAGIC   val response = client.execute(put)
# MAGIC   return response.getStatusLine.getStatusCode()
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming._
# MAGIC import StreamingQueryListener._
# MAGIC 
# MAGIC 
# MAGIC class PushGatewayWipingListener(pushGatewayUrl: String) extends StreamingQueryListener {
# MAGIC   
# MAGIC     override def onQueryStarted(event: QueryStartedEvent): Unit = {
# MAGIC       System.out.println("Query started: " + event.name); }
# MAGIC     override def onQueryTerminated(event: QueryTerminatedEvent ): Unit = {
# MAGIC       wipePushgateway("http://35.223.225.182")
# MAGIC       System.out.println("Query terminated: " + event.id); }
# MAGIC    override def onQueryProgress(event: QueryProgressEvent): Unit = {}
# MAGIC   }
# MAGIC 
# MAGIC val pgWipingListener = new PushGatewayWipingListener("http://35.223.225.182")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming._
# MAGIC import StreamingQueryListener._
# MAGIC import scala.collection.mutable.ListBuffer
# MAGIC import org.apache.spark.internal.Logging
# MAGIC 
# MAGIC def writeToDelta(seq : ListBuffer[String], tableName: String): Unit = {    
# MAGIC     var json_ds = seq.toDS()
# MAGIC     val df = spark.read.json(json_ds)
# MAGIC     df.write.format("delta").mode("append").saveAsTable(tableName)    
# MAGIC }
# MAGIC 
# MAGIC class QueryProgressWritingListener() extends StreamingQueryListener with Logging {
# MAGIC   var json_seq = new ListBuffer[String]()
# MAGIC     override def onQueryStarted(event: QueryStartedEvent): Unit = {}
# MAGIC     override def onQueryTerminated(event: QueryTerminatedEvent ): Unit = {
# MAGIC       if( !json_seq.isEmpty ){
# MAGIC         writeToDelta(json_seq, "dlyle.queryprogress")
# MAGIC       }
# MAGIC     }
# MAGIC     override def onQueryProgress(event: QueryProgressEvent): Unit = {  
# MAGIC       json_seq += event.progress.json   
# MAGIC       if( json_seq.length == 120)
# MAGIC       try{
# MAGIC           writeToDelta(json_seq, "dlyle.queryprogress")                
# MAGIC           json_seq.clear() 
# MAGIC       }catch {
# MAGIC         case t: Throwable => {          
# MAGIC           logError("Error writing QPO to Delta", t)
# MAGIC           throw t
# MAGIC         }
# MAGIC       }      
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC val qpWritingListener = new QueryProgressWritingListener()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.streams.addListener(pgWipingListener)
# MAGIC spark.streams.addListener(qpWritingListener)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.streams.listListeners

# COMMAND ----------


rateDf = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 2000) \
    .option("numPartitions", 8) \
    .load()


# COMMAND ----------

def foreach_batch_function(df, epoch_id):
    df.collect()

# COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", "8")  # keep the size of shuffles small
 
query = (
  rateDf
    .writeStream
    .queryName("WriteToMemory")
    .trigger(processingTime = "1 second")
    .foreachBatch(foreach_batch_function)                
    .start()
)

# COMMAND ----------

# MAGIC %python
# MAGIC from time import sleep
# MAGIC #sleep(300) 
# MAGIC #query.stop()
