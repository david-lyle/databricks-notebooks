# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Accessing Data in S3 Buckets
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lessons, you should be able to:
# MAGIC * Describe how IAM roles provide secure access to data in S3 buckets
# MAGIC * Mount an S3 bucket to DBFS
# MAGIC * Use s3a URLs to directly access buckets
# MAGIC 
# MAGIC The [recently added SAML-backed IAM credential passthrough](https://databricks.com/blog/2019/07/17/how-databricks-iam-credential-passthrough-solves-common-data-authorization-problems.html) provides our most-secure approach to managing data access in large organizations. If a customer wishes to set this up, contact your Databricks representative for configuration details.
# MAGIC 
# MAGIC In this notebook we will demonstrate using [cluster-mounted IAM roles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/iam-roles.html#step-6-launch-a-cluster-with-the-s3-iam-role) to grant data access rights to all users with permission to access a cluster. Some considerations:
# MAGIC 
# MAGIC ### Security
# MAGIC The main benefit of this system is that it is straightforwardly secure. As long as the admin team maps users to EC2 instances correctly, each user has access to the correct set of entitlements.
# MAGIC 
# MAGIC ### Attribution
# MAGIC This system does not allow attribution to users. Because all users on a given EC2 instance share the same Instance Profile, cloud-native audit logs such as AWS CloudTrail can attribute accesses only to the instance, not to the user running code on the instance.
# MAGIC 
# MAGIC ### Ease of administration
# MAGIC This system is easy to administer provided the number of users and entitlements remain small. However, administration becomes increasingly difficult as the organization scales: admins need to ensure that each user accesses only the EC2 instances with the correct Instance Profiles, which may require manual management if the Instance Profiles don’t map cleanly to policies in the admin’s identity management system (such as LDAP or Active Directory).
# MAGIC 
# MAGIC ### Efficiency
# MAGIC This system requires a separate EC2 instance for each entitlement, which quickly becomes expensive as the organization’s permission model becomes more complex. If there are only a few users with a particular entitlement, that EC2 instance will either sit idle most of the day (increasing cost) or have to be stopped and started according to the work schedule of its users (increasing administrative overhead and slowing down users). Because Apache Spark™ distributes work across a cluster of instances that all require the same Instance Profile, the cost of an idle cluster can become quite large.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC In this lesson, we will demonstrate using IAM roles to manage sensitive connection information for an encrypted S3 bucket. In this notebook, we'll demonstrate:
# MAGIC 
# MAGIC 1. [Secure Access to S3 Buckets Using IAM Roles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/iam-roles.html)
# MAGIC 1. Mounting an S3 bucket
# MAGIC 1. Using s3a URLs to directly access buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Simple Storage Service (S3) Bucket Encryption
# MAGIC 
# MAGIC 1. [Setting default encryption](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/default-bucket-encryption.html) for a bucket will encrypt all objects as they're stored
# MAGIC 1. Works with all existing buckets
# MAGIC 1. Provides 2 options:
# MAGIC   1. Amazon S3-managed keys (SSE-S3)
# MAGIC   1. AWS KMS-managed keys (SSE-KMS)
# MAGIC 1. No new charges for default encryption, but [AWS Key Management Services (KMS) charges apply](https://aws.amazon.com/kms/pricing/)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Mount S3 Bucket - Read/List
# MAGIC 
# MAGIC In this section, we'll mount a bucket with our current IAM role, which has list/read permissions on our target S3 bucket.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Any user within the workspace can view and read the files mounted using this key

# COMMAND ----------

# Unmount directory if previously mounted
MOUNTPOINT = "/mnt/s3demo"
if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
  dbutils.fs.unmount(MOUNTPOINT)

# Define the bucket URL
bucketURL = "s3a://awscore-encrypted"

# Mount using the s3a prefix
try:
  dbutils.fs.mount(bucketURL, MOUNTPOINT)
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e

display(dbutils.fs.ls(MOUNTPOINT))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define and display a Dataframe that reads a file from the mounted directory

# COMMAND ----------

salesDF = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv(MOUNTPOINT + "/source/sales.csv"))

display(salesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Filter the Dataframe and display the results

# COMMAND ----------

from pyspark.sql.functions import col

sales2004DF = salesDF.filter(
    (col("ShipDateKey") > 20031231) & (col("ShipDateKey") <= 20041231)
)
display(sales2004DF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Details....
# MAGIC 
# MAGIC 
# MAGIC While we can list and read files with these credentials, our job will abort when we try to write.

# COMMAND ----------

try:
  sales2004DF.write.mode("overwrite").parquet(MOUNTPOINT + "/output/sales2004DF")
except:
  print("Job aborted")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Review
# MAGIC 
# MAGIC - We just used a read-only IAM role to mount an S3 bucket to the DBFS.
# MAGIC - Mounting data to DBFS makes that content available to anyone in that workspace.
# MAGIC - This mounted bucket will persist between sessions with these same permissions.
# MAGIC - In order to gain write privileges, we will need to redefine our IAM role, or switch to a different IAM role with more privileges.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Switch IAM Role
# MAGIC We have already defined another IAM role in this workspace that has root access to files on this bucket. We will attach to a cluster that has this IAM role mounted now.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The mount we defined is still available in the workspace with the same permissions.
# MAGIC 
# MAGIC As we switch clusters, we create a new SparkSession, so all variables are lost. We'll redefine the path for our mounted date and our bucket URL.

# COMMAND ----------

bucketURL = "s3a://awscore-encrypted"
MOUNTPOINT = "/mnt/s3demo"
dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Directly to Files Using s3a URL
# MAGIC 
# MAGIC Here, we'll bypass mounting to directly write to a S3 bucket. This ensures that only users in the workspace that have access to the associated IAM role will be able to write.

# COMMAND ----------

from pyspark.sql.functions import col

(spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(MOUNTPOINT + "/source/sales.csv")
  .filter((col("ShipDateKey") > 20031231) &
          (col("ShipDateKey") <= 20041231))
  .write
  .mode("overwrite")
  .parquet(bucketURL + "/output/sales2004DF")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we still have our bucket mounted with read-only access to the DBFS, we can use this to check that our write was successful.

# COMMAND ----------

dbutils.fs.ls(MOUNTPOINT+"/output")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using the `overwrite` option requires both `put` and `delete` privileges on S3, as the files are actually fully deleted before the new files are written. Granting only the `put` action will prevent file deletion (though this isn't always desired, as we'll see when optimizing Delta Lakes later in the course).

# COMMAND ----------

# MAGIC %md
# MAGIC Because our current IAM role has `delete` rights, we can use the DBFS to remove our written files.

# COMMAND ----------

dbutils.fs.rm("s3a://awscore-encrypted/output/sales2004DF/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Our target directory should now be empty.

# COMMAND ----------

dbutils.fs.ls(MOUNTPOINT + "/output")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Cleaning up mounts
# MAGIC 
# MAGIC If we don't explicitly unmount, the read-only bucket mounted at the beginning of this notebook will remain accessible in the workspace.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Because we only have read/list permissions, this may be desirable, but you should always be careful when mounting any potentially confidential data.

# COMMAND ----------

if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
  dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Online Resources
# MAGIC 
# MAGIC - [Databricks: S3 Docs](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html)
# MAGIC - [Databricks: S3 Encryption Docs](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html#encryption)
# MAGIC - [Databricks: Secure Access to S3 Buckets Using IAM Roles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/iam-roles.html)
# MAGIC - [Databricks: Secure Access to S3 Buckets Across Accounts Using IAM Roles with an AssumeRole Policy](https://docs.databricks.com/administration-guide/cloud-configurations/aws/assume-role.html)
# MAGIC - [Amazon S3: Default Encryption for S3 Buckets](https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html)
# MAGIC - [Amazon S3: Sharing an Object with Others](https://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURL.html)
# MAGIC - [Amazon S3: Bucket Policy Examples](https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html)
# MAGIC - [Protecting Data Using Server-Side Encryption with Amazon S3-Managed Encryption Keys (SSE-S3)](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
