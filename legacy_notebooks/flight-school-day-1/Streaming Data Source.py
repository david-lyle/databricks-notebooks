# Databricks notebook source
# This creates the "team_name" field displayed at the top of the notebook.

dbutils.widgets.text("team_name", "Enter your team's name");

# COMMAND ----------

# The setup you ran at the beginning of this Workshop module created a database for you.
# However, it doesn't have any tables in it yet.

# Let's set this database to be the default, so we don't have to name it in every operation.
# Note that this cell uses Python, but the spark.sql() method enables us to do our work with standard SQL.  The "USE" command specifies the default database.

spark.sql('USE flight_school_fsteam2')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS readings_stream_source;
# MAGIC 
# MAGIC CREATE TABLE readings_stream_source
# MAGIC   ( id INTEGER, 
# MAGIC     reading_time TIMESTAMP, 
# MAGIC     device_type STRING,
# MAGIC     device_id STRING,
# MAGIC     device_operational_status STRING,
# MAGIC     reading_1 DOUBLE,
# MAGIC     reading_2 DOUBLE,
# MAGIC     reading_3 DOUBLE )
# MAGIC   TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Begin generating data
# MAGIC 
# MAGIC Now we're ready to start pumping data into our stream.
# MAGIC 
# MAGIC The cell below generates transaction records into the Delta Lake table.  Our stream queries above will then consume this data.

# COMMAND ----------

# Now let's simulate an application that streams data into our landing_point table

import time

next_row = 0

while(next_row < 12000):
  
  time.sleep(.005)

  next_row += 1
  
  spark.sql(f"""
    INSERT INTO readings_stream_source (
      SELECT * FROM current_readings_labeled
      WHERE id = {next_row} )
  """)
  
# REMEMBER... you will have to come back to this cell later and CANCEL it if it is still running.
# Otherwise, your cluster will run for a long time.

# COMMAND ----------

# Cancel all running streams

for s in spark.streams.active:
  s.stop()
