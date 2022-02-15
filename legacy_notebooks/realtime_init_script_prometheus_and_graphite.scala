// Databricks notebook source
// MAGIC %md
// MAGIC ### [Docs](https://spark.apache.org/docs/latest/monitoring.html#executor-metrics)
// MAGIC 
// MAGIC 
// MAGIC spark confs required for prometheus
// MAGIC * `spark.ui.prometheus.enabled=true`
// MAGIC * `spark.executor.processTreeMetrics.enabled=true`
// MAGIC * `spark.sql.streaming.metricsEnabled=true` -- Streaming use case monitoring

// COMMAND ----------

// MAGIC %md
// MAGIC With the spark confs above enabled and the init script below running we now are sending data to graphite sink (optional for prometheus) AND the spark UI API is Prometheus compatible. This allows Prometheus to query the SparkUI and receive data in prometheus format.
// MAGIC 
// MAGIC `spark.ui.port` must be < 10000 and be an open port. Standard ports are generally blocked from data transmission
// MAGIC 
// MAGIC When spark.ui.port is set, front-end spark_ui is broken see [ES-164266](https://databricks.atlassian.net/browse/ES-164266)
// MAGIC 
// MAGIC * [Spark UI API Docs](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
// MAGIC 
// MAGIC Above is the link to the SparkUI docs BUT in databricks you have to get through the driver proxy to get to the spark UI. Azure and AWS have slightly diff processes for this.
// MAGIC 
// MAGIC ## Azure Spark UI via Proxy (multi-tenant)
// MAGIC `<apiURL>/driver-proxy-api/o/<organization_id>/<cluster_id>/<spark.ui.port>/api/v1/applications/<application_id>/jobs`
// MAGIC 
// MAGIC ## AWS Spark UI via Proxy (single-tenant)
// MAGIC `https://<shard-name>/driver-proxy-api/o/0/<cluster-id>/<spark.ui.port>/api/v1/`

// COMMAND ----------

//Azure
val env = dbutils.notebook.getContext.apiUrl.get
val token = dbutils.notebook.getContext.apiToken.get
val clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
val sparkContextID = spark.conf.get("spark.databricks.sparkContextId")
val orgId = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
val uiPort = spark.conf.get("spark.ui.port")
val apiPath = "applications"

val url = s"$env/driver-proxy-api/o/$orgId/$clusterId/$uiPort/api/v1/$apiPath"

// COMMAND ----------

// MAGIC %python
// MAGIC # Azure Python
// MAGIC import requests, json
// MAGIC class SparkAPI():
// MAGIC   def __init__(self, cluster_id=None):
// MAGIC     self.notebookContext = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
// MAGIC     self.url = self.notebookContext.apiUrl().get()
// MAGIC     self.token = self.notebookContext.apiToken().get()
// MAGIC     self.cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId") if cluster_id is None else cluster_id
// MAGIC     self.org_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
// MAGIC     self.uiPort = spark.conf.get("spark.ui.port")
// MAGIC     self.headers = {"Authorization": f"Bearer {self.token}"}
// MAGIC     self.url = f"{self.url}/driver-proxy-api/o/{self.org_id}/{self.cluster_id}/{self.uiPort}/api/v1"
// MAGIC 
// MAGIC 
// MAGIC   def get(self, api_path):
// MAGIC     api_url = f"{self.url}/{api_path}"
// MAGIC     response = requests.get(url=api_url, headers=self.headers)
// MAGIC     return json.loads(response.text)

// COMMAND ----------

//AWS
val uiPort = spark.conf.get("spark.ui.port")
val env = dbutils.notebook.getContext.apiUrl.get
val token = dbutils.notebook.getContext.apiToken.get
val clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
val url = s"https://$env/driver-proxy-api/o/0/$clusterId/$uiPort/api/v1/"

// COMMAND ----------

// EXAMPLE INIT WITH GRAPHITE AND PROMETHEUS
// ADD CLUSTERIP to end of prefix but like ".192_168..."
dbutils.fs.put("/databricks/init_scripts/overwatch_proto_v2.sh", """
#!/bin/bash

OVERWATCH_RELAY=<RELAY ENDPOINT OR LOAD BALANCER>
WORKSPACE=<WORKSPACE_NAME>

if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  IS_DRIVER="driver"
else
  IS_DRIVER="worker"
fi

IP_CLEAN=$( echo ${DB_CONTAINER_IP} | tr '.' '_' )

LOG_DIR=/dbfs/cluster-logs/${DB_CLUSTER_ID}/csv_metrics/${DB_CONTAINER_IP}
if [ ! -d "/dbfs/cluster-logs/${DB_CLUSTER_ID}/csv_metrics/${DB_CONTAINER_IP}" ]
then
  mkdir -p $LOG_DIR
fi
TMP_SCRIPT=/tmp/custom_metrics.sh

cat >"$TMP_SCRIPT" <<EOL

master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource

*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=${OVERWATCH_RELAY}
*.sink.graphite.port=2003
*.sink.graphite.period=10
*.sink.graphite.unit=seconds
*.sink.graphite.prefix=v1.${WORKSPACE}.cluster_${DB_CLUSTER_ID}.spark.raw
*.sink.prometheusServlet.class=org.apache.spark.metrics.sing.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
EOL

cat "$TMP_SCRIPT" >> /databricks/spark/conf/metrics.properties

apt-get update -y
apt install collectd collectd-utils -y

cat >"/etc/collectd/collectd.conf" <<EOL
Hostname "${IP_CLEAN}"
FQDNLookup false
BaseDir "/local_disk0/tmp"

LoadPlugin cpu
LoadPlugin CPUFreq
LoadPlugin df
LoadPlugin entropy
LoadPlugin interface
LoadPlugin load
LoadPlugin memory
LoadPlugin mysql
LoadPlugin processes
LoadPlugin rrdtool
LoadPlugin users
LoadPlugin write_graphite

<Plugin df>
    Device "/dev/mapper/vg-lv"
    MountPoint "/local_disk0"
    FSType "ext4"
</Plugin>

<Plugin interface>
    Interface "ens3"
    IgnoreSelected false
</Plugin>

<Plugin write_graphite>
    <Node "graphing">
        Host "${OVERWATCH_RELAY}"
        Port "2003"
        Protocol "tcp"
        LogSendErrors true
        Prefix "v1.${WORKSPACE}.cluster_${DB_CLUSTER_ID}.machine.raw.${IS_DRIVER}."
        StoreRates true
        AlwaysAppendDS false
        EscapeCharacter "_"
    </Node>
</Plugin>
EOL

service collectd restart

""", true)

// COMMAND ----------

// MAGIC %md
// MAGIC Resulting Init Script
// MAGIC ```
// MAGIC "
// MAGIC #!/bin/bash
// MAGIC 
// MAGIC OVERWATCH_RELAY=<REDACTED>
// MAGIC WORKSPACE=<REDACTED>
// MAGIC 
// MAGIC if [[ $DB_IS_DRIVER = "TRUE" ]]; then
// MAGIC   IS_DRIVER="driver"
// MAGIC else
// MAGIC   IS_DRIVER="worker"
// MAGIC fi
// MAGIC 
// MAGIC IP_CLEAN=$( echo ${DB_CONTAINER_IP} | tr '.' '_' )
// MAGIC 
// MAGIC LOG_DIR=/dbfs/cluster-logs/${DB_CLUSTER_ID}/csv_metrics/${DB_CONTAINER_IP}
// MAGIC if [ ! -d "/dbfs/cluster-logs/${DB_CLUSTER_ID}/csv_metrics/${DB_CONTAINER_IP}" ]
// MAGIC then
// MAGIC   mkdir -p $LOG_DIR
// MAGIC fi
// MAGIC TMP_SCRIPT=/tmp/custom_metrics.sh
// MAGIC 
// MAGIC cat >"$TMP_SCRIPT" <<EOL
// MAGIC 
// MAGIC master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
// MAGIC worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
// MAGIC driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
// MAGIC executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
// MAGIC 
// MAGIC *.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
// MAGIC *.sink.graphite.host=${OVERWATCH_RELAY}
// MAGIC *.sink.graphite.port=2003
// MAGIC *.sink.graphite.period=10
// MAGIC *.sink.graphite.unit=seconds
// MAGIC *.sink.graphite.prefix=v1.${WORKSPACE}.cluster_${DB_CLUSTER_ID}.spark.raw
// MAGIC EOL
// MAGIC 
// MAGIC cat "$TMP_SCRIPT" >> /databricks/spark/conf/metrics.properties
// MAGIC 
// MAGIC apt-get update -y
// MAGIC apt install collectd collectd-utils -y
// MAGIC 
// MAGIC cat >"/etc/collectd/collectd.conf" <<EOL
// MAGIC Hostname "${IP_CLEAN}"
// MAGIC FQDNLookup false
// MAGIC BaseDir "/local_disk0/tmp"
// MAGIC 
// MAGIC LoadPlugin cpu
// MAGIC LoadPlugin CPUFreq
// MAGIC LoadPlugin df
// MAGIC LoadPlugin entropy
// MAGIC LoadPlugin interface
// MAGIC LoadPlugin load
// MAGIC LoadPlugin memory
// MAGIC LoadPlugin mysql
// MAGIC LoadPlugin processes
// MAGIC LoadPlugin rrdtool
// MAGIC LoadPlugin users
// MAGIC LoadPlugin write_graphite
// MAGIC 
// MAGIC <Plugin df>
// MAGIC     Device "/dev/mapper/vg-lv"
// MAGIC     MountPoint "/local_disk0"
// MAGIC     FSType "ext4"
// MAGIC </Plugin>
// MAGIC 
// MAGIC <Plugin interface>
// MAGIC     Interface "ens3"
// MAGIC     IgnoreSelected false
// MAGIC </Plugin>
// MAGIC 
// MAGIC <Plugin write_graphite>
// MAGIC     <Node "graphing">
// MAGIC         Host "${OVERWATCH_RELAY}"
// MAGIC         Port "2003"
// MAGIC         Protocol "tcp"
// MAGIC         LogSendErrors true
// MAGIC         Prefix "v1.${WORKSPACE}.cluster_${DB_CLUSTER_ID}.machine.raw.${IS_DRIVER}."
// MAGIC         StoreRates true
// MAGIC         AlwaysAppendDS false
// MAGIC         EscapeCharacter "_"
// MAGIC     </Node>
// MAGIC </Plugin>
// MAGIC EOL
// MAGIC 
// MAGIC service collectd restart
// MAGIC 
// MAGIC #sed -i 's|.publicFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n|.publicFile.layout.ConversionPattern=~~~%d{yy/MM/dd HH:mm:ss}~%p~%c{1}:~%m!!!|g' /databricks/spark/dbconf/log4j/driver/log4j.properties
// MAGIC #sed -i 's|ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n|ConversionPattern=~~~%d{yy/MM/dd HH:mm:ss}~%p~%c{1}:~%m!!!|g' /databricks/spark/dbconf/log4j/executor/log4j.properties
// MAGIC 
// MAGIC "
// MAGIC ```

// COMMAND ----------

dbutils.fs.head("dbfs:/databricks/init_scripts/overwatch_proto_v2.sh")
