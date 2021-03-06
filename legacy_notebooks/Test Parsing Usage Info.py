# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Accounting on Databricks GCP <img src="/files/images/320px_Databricks_Logo.png" width=160 height=160 />
# MAGIC 
# MAGIC #### 1. Enforcing Tags with Cluster Policies
# MAGIC Users (Belong to a Group) -> Groups (Associated with a Cost Center) -> Cluster Policy (Enforce Cost Center Tagging) -> Clusters (Tagged with Correct Cost Center) <P>
# MAGIC [Manage Cluster Policies](https://docs.gcp.databricks.com/administration-guide/clusters/policies.html)
# MAGIC #### 2. Usage Reporting
# MAGIC [View billable usage using the account console](https://docs.gcp.databricks.com/administration-guide/account-settings-gcp/usage.html)
# MAGIC #### 3. Downloading CSV - Currently Manual (Accounts API in Private Preview)
# MAGIC [AWS Example of Download via Account API](https://docs.databricks.com/administration-guide/account-settings/billable-usage-download-api.html)
# MAGIC #### 4. Parsing CSV Using Databricks
# MAGIC [Analyze billable usage log data](https://docs.databricks.com/administration-guide/account-settings/usage-analysis.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC use  dlyle

# COMMAND ----------

dbutils.fs.rm(checkpoint_path,recurse=True)
dbutils.fs.rm(write_path, recurse=True)

spark.sql("""
drop table if exists billingusage
""")

spark.sql("""
create table billingusage(workspaceId string, timestamp timestamp, 
                          clusterId string, clusterName string, 
                          clusterNodeType string, clusterOwnerUserId string, 
                          sku string, dbus double, machineHours double, 
                          clusterOwnerUserName string, tags map<string, string>, cost double)
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

usagefilePath = "dbfs:/FileStore/shared_uploads/david.lyle@databricks.com/itemizedusage"
checkpoint_path = 'dbfs:/dlyle/itemizedusage/_checkpoints'
write_path = 'dbfs:/dlyle/itemizedusage/delta/usage_data'
schema_location = 'dbfs:/dlyle/itemizedusage/schema'


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
           
           .drop("userId")
           .drop("clusterCustomTags")           
          )

streamingQuery = (usageDF.writeStream 
  .option("mergeSchema", "true") 
  .option("checkpointLocation", checkpoint_path)  
  .option("maxFilesPerTrigger", 1)                  
  .trigger(once=True) 
  .foreachBatch(drop_duplicates)  
  .start()                  
   )          



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from billingusage

# COMMAND ----------

# Build usage schema
usageSchema = StructType([
  StructField("workspaceId", StringType(), False),
  StructField("timestamp", TimestampType(), False),
  StructField("clusterId", StringType(), False),
  StructField("clusterName", StringType(), False),
  StructField("clusterNodeType", StringType(), False),
  StructField("clusterOwnerUserId", StringType(), False),
  StructField("clusterCustomTags", StringType(), False),
  StructField("sku", StringType(), False),
  StructField("dbus", FloatType(), False),
  StructField("machineHours", FloatType(), False),
  StructField("clusterOwnerUserName", StringType(), False),
  StructField("tags", StringType(), False)
])

#Read file to dataframe - dbfs:/FileStore/2022_01_2022_01.csv
usagefilePath = "dbfs:/FileStore/shared_uploads/david.lyle@databricks.com/itemizedusage/2021_04_2022_03.csv"

# Instantiate and cache the usage dataframe
df = (spark.read
      .option("header", "true")
      .option("escape", "\"")
      .schema(usageSchema)
      .csv(usagefilePath)
      )
df.createOrReplaceTempView("usage")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), workspaceId, timestamp, clusterId from usage group by workspaceId, timestamp, clusterId order by count(1) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from usage where workspaceId = '1559018344558186' and timestamp = '2021-12-21T14:59:59.000+0000' and clusterId = '1221-120530-dingy494'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select clusterName, sku, sum(dbus) as dubs, round(sum(cost),2) as cost from usage where tags.CostCenter == 10011 group by clusterName, sku

# COMMAND ----------

# MAGIC %sql
# MAGIC select coalesce(tags.CostCenter,"NONE") as costCenter, sku, sum(dbus) as dbus, round(sum(cost),2) as cost_in_dollars from usage group by costCenter, sku order by cost_in_dollars desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(clusterName) as clusterName, clusterOwnerUsername from usage where tags.CostCenter is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select clusterName, sku, round(sum(cost),2) as cost_in_dollars from usage group by clusterName, sku order by cost_in_dollars desc
