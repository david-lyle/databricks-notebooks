# Databricks notebook source
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
# MAGIC     // Modify StreamingQueryListener Methods 
# MAGIC     override def onQueryStarted(event: QueryStartedEvent): Unit = {
# MAGIC       System.out.println("Query started: " + event.name); }
# MAGIC     override def onQueryTerminated(event: QueryTerminatedEvent ): Unit = {
# MAGIC       wipePushgateway("http://35.223.225.182")
# MAGIC       System.out.println("Query terminated: " + event.id); }
# MAGIC    override def onQueryProgress(event: QueryProgressEvent): Unit = {
# MAGIC       //System.out.println("Query made progress: " + event.progress);
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC val pgWipingListener = new PushGatewayWipingListener("http://35.223.225.182")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.streams.addListener(pgWipingListener)

# COMMAND ----------


rateDf = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 2000) \
    .option("numPartitions", 8) \
    .load()


# COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", "8")  # keep the size of shuffles small

query = (
  rateDf
    .writeStream
    .format("memory")        # memory = store in-memory table 
    .queryName("rates")     # rates = name of the in-memory table
    .outputMode("append") 
    .start()
)

# COMMAND ----------

# MAGIC %python
# MAGIC from time import sleep
# MAGIC sleep(300) 
# MAGIC query.stop()
