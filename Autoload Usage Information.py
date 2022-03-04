# Databricks notebook source
dbutils.widgets.text('Database', '')
dbutils.widgets.text('Checkpoint Path', '')
dbutils.widgets.text('Schema Location', '')
dbutils.widgets.text('Autoload File Path', '')

# COMMAND ----------

#Call this in separate cell to begin from scratch
def deleteData():
    dbutils.fs.rm(getArgument('Checkpoint Path'),recurse=True)

    spark.sql("""
    drop table if exists billingusage
    """)


# COMMAND ----------

#deleteData()

# COMMAND ----------

spark.sql("create database if not exists " + getArgument('Database'))
spark.sql("use " + getArgument('Database'))

# COMMAND ----------

spark.sql("""
create table if not exists billingusage(workspaceId string, timestamp timestamp, 
                          clusterId string, clusterName string, 
                          clusterNodeType string, clusterOwnerUserId string, 
                          sku string, dbus double, machineHours double, 
                          clusterOwnerUserName string, tags map<string, string>, cost double,
                          billingDate date) partitioned by (billingDate)
""")

# COMMAND ----------

from delta.tables import *

def drop_duplicates(df, epoch):  
    df = df.dropDuplicates(['workspaceId','timestamp','clusterId', 'dbus'])
    table = DeltaTable.forName(spark, 
      'billingusage')
    dname = "destination"
    uname = "updates"
    dup_columns = ['workspaceId','timestamp', 'clusterId', 'dbus']
    merge_condition = " AND ".join([f"{dname}.{col} = {uname}.{col}"
      for col in dup_columns])
    print(merge_condition)
    table.alias(dname).merge(df.alias(uname), merge_condition)\
     .whenNotMatchedInsertAll().execute()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func

usagefilePath = getArgument('Autoload File Path')
checkpoint_path = getArgument('Checkpoint Path')
schema_location = getArgument('Schema Location')

df = (spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "csv") 
  .option("cloudFiles.schemaLocation", schema_location) 
  .option("cloudFiles.schemaHints", "timestamp timestamp")
  .option("escape", "\"")  
  .load(usagefilePath) 
   )


#Transform Data to Usable form and add cost
usageDF = (df.select("workspaceId",
                     "timestamp",
                     "clusterId",
                     "clusterName",
                     "clusterNodeType",
                     "clusterOwnerUserId",
                     "clusterCustomTags",
                     when(col("sku") == "STANDARD_INTERACTIVE_OPSEC", "All Purpose Compute")
                     .when(col("sku") == "STANDARD_AUTOMATED_NON_OPSEC", "Jobs Compute")
                     .when(col("sku") == "STANDARD_INTERACTIVE_NON_OPSEC", "All Purpose Compute")
                     .when(col("sku") == "LIGHT_AUTOMATED_NON_OPSEC", "Jobs Compute Light")
                     .when(col("sku") == "STANDARD_AUTOMATED_OPSEC", "Jobs Compute")
                     .when(col("sku") == "STANDARD_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "STANDARD_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "PREMIUM_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "PREMIUM_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "ENTERPRISE_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "ENTERPRISE_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)", "All Purpose Compute")
                     .otherwise(col("sku")).alias("sku"),
                     "dbus",
                     "machineHours",
                     "clusterOwnerUserName",
                     "tags")
           .withColumn("clusterOwnerUserName", when(col("clusterOwnerUserName").isNotNull(), col("clusterOwnerUserName")).otherwise("david.lyle@databricks.com"))
           .withColumn("tags", when(col("tags").isNotNull(), col("tags")).otherwise(col("clusterCustomTags")))
           .withColumn("tags", from_json("tags", MapType(StringType(), StringType())).alias("tags"))
           .withColumn("cost", 
                       when(col("sku") == "Jobs Compute", col("dbus") * .22)
                       .when(col("sku") == "All Purpose Compute", col("dbus") * .55)                       
                       .otherwise(.55))
           .withColumn("billingDate", to_date(col("timestamp")))
           .drop("userId")
           .drop("clusterCustomTags")           
          )

streamingQuery = (usageDF.writeStream 
  .option("mergeSchema", "true") 
  .option("checkpointLocation", checkpoint_path)                   
  .trigger(once=True) 
  .foreachBatch(drop_duplicates)  
  .queryName("WriteBilling")
  .start()                  
   )          

# COMMAND ----------

# MAGIC %sql
# MAGIC --optimize table
# MAGIC optimize billingusage 
