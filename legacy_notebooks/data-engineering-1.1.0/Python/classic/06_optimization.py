# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Raw to Bronze to Silver Delta Architecture

# COMMAND ----------

# MAGIC %run ./04_main

# COMMAND ----------

# MAGIC %md
# MAGIC ## The tiny files problem

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display Silver Table Partitions

# COMMAND ----------

display(dbutils.fs.ls(silverPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the Files in a Partition

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the Files in a Partition

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize using Z-Order

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
