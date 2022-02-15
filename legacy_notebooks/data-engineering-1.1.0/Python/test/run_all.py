# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Harness for Solution Notebooks

# COMMAND ----------

dbutils.fs.rm("/dbacademy/dbacademy", recurse=True)

# COMMAND ----------

dbutils.notebook.run("../solutions/plus/00_ingest_raw", 500)
dbutils.notebook.run("../solutions/plus/01_raw_to_bronze", 500)
dbutils.notebook.run("../solutions/plus/02_bronze_to_silver", 500)
dbutils.notebook.run("../solutions/plus/03_silver_update", 500)
dbutils.notebook.run("../solutions/plus/04_silver_to_gold", 500)
dbutils.notebook.run("../solutions/plus/04_silver_to_gold_lab", 500)
dbutils.notebook.run("../solutions/plus/05_schema_enforcement", 500)
dbutils.notebook.run("../solutions/plus/06_schema_evolution", 500)
dbutils.notebook.run("../solutions/classic/00_ingest_raw", 500)
dbutils.notebook.run("../solutions/classic/01_raw_to_bronze", 500)
dbutils.notebook.run("../solutions/classic/02_bronze_to_silver", 500)
dbutils.notebook.run("../solutions/classic/03_silver_update", 500)
dbutils.notebook.run("../solutions/classic/04_main", 500)
dbutils.notebook.run("../solutions/classic/05_compliance", 500)
dbutils.notebook.run("../solutions/classic/06_optimization", 500)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
