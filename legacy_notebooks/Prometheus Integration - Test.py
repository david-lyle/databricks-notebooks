# Databricks notebook source
dbutils.fs.put("dbfs:/dlyle/install-prometheus.sh",
"""
#!/bin/bash
cat <<EOF > /databricks/spark/conf/metrics.properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
EOF
cat >/databricks/driver/conf/00-custom-spark.conf <<EOF
[driver] {  
  spark.sql.streaming.metricsEnabled = "true"
  spark.metrics.appStatusSource.enabled = "true"
}
EOF
""", True)


# COMMAND ----------

dbutils.fs.put("dbfs:/dlyle/install-banzaicloud.sh",
"""
#!/bin/bash
cat <<EOF > /databricks/spark/conf/metrics.properties
# Enable Prometheus for all instances by class name
*.sink.prometheus.class=org.apache.spark.banzaicloud.metrics.sink.PrometheusSink
# Prometheus pushgateway address
*.sink.prometheus.pushgateway-address-protocol=http
*.sink.prometheus.pushgateway-address=35.224.11.4
# Support for JMX Collector (version 2.3-2.0.0 +)
*.sink.prometheus.enable-dropwizard-collector=false
*.sink.prometheus.enable-jmx-collector=false
#*.sink.prometheus.jmx-collector-config=/opt/spark/conf/monitoring/jmxCollector.yaml
# Enable HostName in Instance instead of Appid (Default value is false i.e. instance=${appid})
*.sink.prometheus.enable-hostname-in-instance=true
# Enable JVM metrics source for all instances by class name
#*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
#*.source.jvm.class=org.apache.spark.metrics.source.JvmSource
EOF
cat >/databricks/driver/conf/00-custom-spark.conf <<EOF
[driver] {
  spark.metrics.namespace = "${DB_CLUSTER_NAME}"
  spark.sql.streaming.metricsEnabled = "true"
  spark.metrics.appStatusSource.enabled = "true"
}
EOF
wget --quiet -O /databricks/jars/spark-metrics-assembly-3.1-1.0.0.jar https://github.com/dlyle65535/spark-metrics/raw/3.1-1.0.0/maven-repo/releases/com/banzaicloud/spark-metrics_3.1/3.1-1.0.0/spark-metrics-assembly-3.1-1.0.0.jar
""", True)

# COMMAND ----------

dbutils.fs.ls("dbfs:/cluster-logs/0119-213156-fax211/driver/")

# COMMAND ----------

dbutils.fs.head("dbfs:/cluster-logs/0119-213156-fax211/driver/stdout")

# COMMAND ----------

spark.conf.get("spark.ui.port")
