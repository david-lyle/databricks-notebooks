# Databricks notebook source
#Read from fe-dev-sandbox-audit/workspaceId=3888031910422875
sourceBucket = "gs://fe-dev-sandbox-audit/workspaceId=3888031910422875/"

streamSchema = spark.read.json(sourceBucket).schema

streamDF = (
    spark
    .readStream
    .format("json")
    .schema(streamSchema)
    .load(sourceBucket)
)      


# COMMAND ----------

sinkBucket = "dbfs:/delta/dml"

(streamDF
.writeStream
.format("delta")
.partitionBy("date")
.outputMode("append")
.option("checkpointLocation", "{}/checkpoints/bronze".format(sinkBucket))
.option("path", "{}/streaming/bronze".format(sinkBucket))
.option("mergeSchema", True)
.trigger(once=True)
.start()
)   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS audit_logs

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS audit_logs.bronze
USING DELTA
LOCATION '{}/streaming/bronze'
""".format(sinkBucket)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_logs.bronze
