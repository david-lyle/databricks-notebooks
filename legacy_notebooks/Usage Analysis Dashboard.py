# Databricks notebook source
# MAGIC %md 
# MAGIC ## Databricks Usage Analytics Dashboard
# MAGIC This dashboard leverages the usage data sent directly to your designated object store. Follow the instructions below to execute a daily refresh of each chart and report.
# MAGIC 
# MAGIC #### How to use this notebook
# MAGIC 1. Enter the path to your object store (bucket) name in the usagefilePath string in the the Cmd 2 cell. The syntax has been provided for your reference. 
# MAGIC 2. Fill in the fields in the widget that precedes this cell, including commit dollars (if you have upfront commit with Databricks), date range, your unit DBU price for each compute type (SKU Price), the cluster tag key you want to use to break down usage and cost, time period granularity, and the usage measure (spend, DBUs, cumulative spend, cumulative DBUs).
# MAGIC 3. Click **Run All** from the Cmd 2 cell or the notebook toolbar. 
# MAGIC 
# MAGIC ###### Note:
# MAGIC This notebook assumes that your organization is paying the same unit DBU price across all of your workspaces. If you have multiple workspaces, each on a different pricing tier, these views do not show accurate dollar spend.

# COMMAND ----------

# Enter the path to your usage file. For example: s3a://my-bucket/delivery/path/csv/dbus/
usagefilePath = "dbfs:/FileStore/2022_01_2022_01.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do not modify the cells that follow

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func

# Build usage schema
usageSchema = StructType([
  StructField("workspaceId", StringType(), False),
  StructField("timestamp", DateType(), False),
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

# Instantiate and cache the usage dataframe
df = (spark.read
      .option("header", "true")
      .option("escape", "\"")
      .schema(usageSchema)
      .csv(usagefilePath)
      )

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
                     .when(col("sku") == "LIGHT_AUTOMATED_OPSEC", "Jobs Compute Light")
                     .when(col("sku") == "STANDARD_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "STANDARD_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "STANDARD_JOBS_LIGHT_COMPUTE", "Jobs Compute Light")
                     .when(col("sku") == "PREMIUM_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "PREMIUM_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "PREMIUM_JOBS_LIGHT_COMPUTE", "Jobs Compute Light")
                     .when(col("sku") == "ENTERPRISE_ALL_PURPOSE_COMPUTE", "All Purpose Compute")
                     .when(col("sku") == "ENTERPRISE_JOBS_COMPUTE", "Jobs Compute")
                     .when(col("sku") == "ENTERPRISE_JOBS_LIGHT_COMPUTE", "Jobs Compute Light")
                     .otherwise(col("sku")).alias("sku"),
                     "dbus",
                     "machineHours",
                     "clusterOwnerUserName",
                     "tags")
           .withColumn("tags", when(col("tags").isNotNull(), col("tags")).otherwise(col("clusterCustomTags")))
           .withColumn("tags", from_json("tags", MapType(StringType(), StringType())).alias("tags"))
           .drop("userId")
           .cache()
          )

# Create temp table for sql commands
usageDF.createOrReplaceTempView("usage")

# COMMAND ----------

# Instantiate Widgets for dynamic filtering
import datetime
dbutils.widgets.removeAll()

# Date Window Filter
now = datetime.datetime.now()
dbutils.widgets.text("Date - End", now.strftime("%Y-%m-%d"))
dbutils.widgets.text("Date - Beginning", now.strftime("%Y-%m-%d"))

# Sku Pricing. Creates a text widget for each distinct value in a customer's account.
skus = spark.sql("select distinct(sku) from usage").rdd.map(lambda row : row[0]).collect()
for sku in skus:  # Print text boxes for each distinct customer sku
  dbutils.widgets.text("SKU Price - " + sku, ".00")

# Time Unit for graphs over time
dbutils.widgets.dropdown("Time Unit", "Day", ["Day", "Month"])
timeUnit = "Time"

# Commit Amount
dbutils.widgets.text("Commit Dollars", "00.00")
commit = getArgument("Commit Dollars")

# Tag Key
tags = spark.sql("select distinct(explode(map_keys(tags))) from usage").rdd.map(lambda row : row[0]).collect()
if len(tags) > 0:
  defaultTag = tags[0]
  dbutils.widgets.dropdown("Tag Key", str(defaultTag), [str(x) for x in tags])

# Usage Type
dbutils.widgets.dropdown("Usage", "Spend", ["Spend", "DBUs", "Cumulative Spend", "Cumulative DBUs"])

# COMMAND ----------

# Create a dataframe from sku names and their rates. This gets joined to the usage dataframe to get spend.
skuVals = [str(sku) for sku in skus]
wigVals = [getArgument("SKU Price - " + sku) for sku in skus]

skuZip = list(zip(skuVals, wigVals)) # Create a list object from each sku and its corresponding rate to parallelize into an RDD

skuRdd = sc.parallelize(skuZip) # Create RDD

skuSchema = StructType([
  StructField("sku", StringType(), True),
  StructField("rate", StringType(), True)])

skuDF = spark.createDataFrame(skuRdd, skuSchema) # Create a dataframe

# COMMAND ----------

timeUnitFormat = "yyyy-MM-dd" if getArgument("Time Unit") == "Day" else "yyyy-MM"

# A large dataframe, containing spend, DBUs, cumulative spend and cumulative DBUs for every time, commit, sku and tag value
globalDF = (usageDF
            .filter(usageDF.timestamp.between(getArgument("Date - Beginning"), getArgument("Date - End")))
            .join(skuDF, "Sku")
            .withColumn("Spend", usageDF["dbus"] * skuDF["rate"])
            .withColumn("Commit", lit(getArgument("Commit Dollars")))
            .withColumn("Tag", usageDF["tags." + getArgument("Tag Key")])
            .select(date_format("timestamp", timeUnitFormat).alias(timeUnit), "Spend", "dbus", "Sku", "Commit", "Tag")
            .groupBy(timeUnit, "Commit", "Sku", "Tag")
            .sum("Spend", "dbus")
            .withColumnRenamed("sum(dbus)", "DBUs")
            .withColumnRenamed("sum(Spend)", "Spend")
            .orderBy(timeUnit)
           )

# A function to generate smaller dataframes considering only one of commit, sku or tag value and calculate the cumulation values for spend and DBUs
def usageBy(columnName):
  
  # Window function to get cumulative spend/DBUs
  cumulativeUsage = Window \
  .orderBy(timeUnit) \
  .partitionBy(columnName) \
  .rowsBetween(Window.unboundedPreceding, 0)
  
  # Instead of having no rows when a time period has no data for a value of the selected column,
  # we add this row with zero spend & DBUs and append the correct cumulation.
  # This is so the graphs don't interpret the cumulation value as zero.
  zeros = globalDF.select(columnName).distinct().withColumn("Spend", lit(0)).withColumn("DBUs", lit(0))
  zerosByTime = globalDF.select(timeUnit).distinct().crossJoin(zeros)
  
  return (globalDF
          .select(timeUnit, columnName, "Spend", "DBUs")
          .union(zerosByTime)
          .groupBy(timeUnit, columnName)
          .sum("Spend", "DBUs")
          .withColumnRenamed("sum(DBUs)", "DBUs")
          .withColumnRenamed("sum(Spend)", "Spend")
          .withColumn("Cumulative Spend", func.sum(func.col("Spend")).over(cumulativeUsage))
          .withColumn("Cumulative DBUs", func.sum(func.col("DBUs")).over(cumulativeUsage))
          .select(timeUnit, columnName, getArgument("Usage"))
          .withColumnRenamed(getArgument("Usage"), "Usage")
          .orderBy(timeUnit)
         )

display(globalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Usage Reporting
# MAGIC 1. Usage over time
# MAGIC 2. Usage by SKU over time
# MAGIC 2. Usage by selected tag over time

# COMMAND ----------

commitMsg = ""
if getArgument("Usage") == "Cumulative Spend":
  commitMsg = "&nbsp;"*8 + "Commit = $" + getArgument("Commit Dollars")

displayHTML("<center><h2>{0} over time{1}</h2></center>".format(getArgument("Usage"), commitMsg))

# COMMAND ----------

# DBTITLE 1,Usage over time
display(usageBy("Commit"))

# COMMAND ----------

displayHTML("<center><h2>{} by SKU over time</h2></center>".format(getArgument("Usage")))

# COMMAND ----------

# DBTITLE 1,Usage by SKU over time
display(usageBy("Sku"))

# COMMAND ----------

displayHTML("<center><h2>{0} by {1} over time</h2></center>".format(getArgument("Usage"), getArgument("Tag Key")))

# COMMAND ----------

# DBTITLE 1,Usage by tag over time
display(usageBy("Tag"))
