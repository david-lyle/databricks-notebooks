# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # GCP Databricks Prometheus Integration Two Ways
# MAGIC - [BanzaiCloud Spark Metrics](https://github.com/banzaicloud/spark-metrics)
# MAGIC - [Spark Monitoring](https://spark.apache.org/docs/3.0.0-preview/monitoring.html)
# MAGIC - [Prometheus Docs](https://prometheus.io/docs/introduction/overview/)
# MAGIC - [Prometheus Pushgateway](https://github.com/prometheus/pushgateway)
# MAGIC 
# MAGIC #### 1) Preliminary Setup of Prometheus and the Pushgateway
# MAGIC #### 2) BanzaiCloud Spark Metrics Setup
# MAGIC #### 3) Spark Prometheus Servelet Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Preliminary Setup of Prometheus and the Pushgateway on GKE
# MAGIC 
# MAGIC #### Setup on GKE 
# MAGIC 1) Create GKE Cluster (or use existing)
# MAGIC 
# MAGIC 2) Add your IP to 'Control Plane Authorized Networks'
# MAGIC 
# MAGIC 3) Get cluster credentials ```gcloud container clusters get-credentials <cluster_name> --region <region> --project <project>``` 
# MAGIC #### Install Prometheus Services to Existing GKE Cluster
# MAGIC 1) Enable the prometheus-community repo for helm - ```helm repo add prometheus-community https://prometheus-community.github.io/helm-charts```
# MAGIC 
# MAGIC 2) Update the repo - ```helm repo update```
# MAGIC 
# MAGIC 3) Create prometheus_values.yaml as shown below:
# MAGIC ```
# MAGIC alertmanager:
# MAGIC   service:
# MAGIC     type: LoadBalancer
# MAGIC 
# MAGIC server:
# MAGIC   service:
# MAGIC     type: LoadBalancer
# MAGIC 
# MAGIC pushgateway:
# MAGIC   service:
# MAGIC     type: LoadBalancer
# MAGIC     servicePort: 80
# MAGIC ```
# MAGIC 
# MAGIC 4) Run helm install: ```helm install <installation name - i.e. dlyle-prometheus> -f prometheus_values.yaml -n <namespace - i.e. prometheus-monitoring> --create-namespace prometheus-community/prometheus```
# MAGIC 
# MAGIC 5) Get Service Ports: ```kubectl get svc -n prometheus-monitoring``` - capture the pushgateway port to add to init script below:
# MAGIC 
# MAGIC Note: In production, use a proper ingress- it is inefficient to setup 3 load balancers for one set of services.

# COMMAND ----------

# MAGIC %md
# MAGIC # BanzaiCloud Spark Metrics Setup
# MAGIC 
# MAGIC #### Build and Host BanzaiCloud Spark Metrics for your version of Spark/Scala. See instructions [https://github.com/banzaicloud/spark-metrics](here).
# MAGIC - I'm hosting a binary for 3.1.2/2.1.2 on [https://github.com/dlyle65535/spark-metrics/raw/3.1-1.0.0/maven-repo/releases/com/banzaicloud/spark-metrics_3.1/3.1-1.0.0/spark-metrics-assembly-3.1-1.0.0.jar](github).
# MAGIC 
# MAGIC #### Create BanzaiCloud init script as shown below:

# COMMAND ----------

#Create Init script
dbutils.fs.put("dbfs:/dlyle/install-banzaicloud.sh","""
#!/bin/bash
cat <<EOF > /databricks/spark/conf/metrics.properties
# Enable Prometheus for all instances by class name
*.sink.prometheus.class=org.apache.spark.banzaicloud.metrics.sink.PrometheusSink
# Prometheus pushgateway address
*.sink.prometheus.pushgateway-address-protocol=http
*.sink.prometheus.pushgateway-address=35.223.225.182
# Support for JMX Collector (version 2.3-2.0.0 +)
*.sink.prometheus.enable-dropwizard-collector=true
*.sink.prometheus.enable-jmx-collector=true
*.sink.prometheus.jmx-collector-config=/databricks/spark/conf/jmxCollector.yaml
# Enable HostName in Instance instead of Appid (Default value is false i.e. instance=${appid})
*.sink.prometheus.enable-hostname-in-instance=true
# Enable JVM metrics source for all instances by class name
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
*.source.jvm.class=org.apache.spark.metrics.source.JvmSource
EOF
cat >/databricks/driver/conf/00-custom-spark.conf <<EOF
[driver] {
#  spark.metrics.namespace = "${DB_CLUSTER_NAME}"
  spark.sql.streaming.metricsEnabled = "true"
  spark.metrics.appStatusSource.enabled = "true"
}
EOF
cat >/databricks/spark/conf/jmxCollector.yaml << EOF
    lowercaseOutputName: false
    lowercaseOutputLabelNames: false
    whitelistObjectNames: ["*:*"]
EOF
wget --quiet -O /databricks/jars/spark-metrics-assembly-3.1-1.0.0.jar https://github.com/dlyle65535/spark-metrics/raw/3.1-1.0.0/maven-repo/releases/com/banzaicloud/spark-metrics_3.1/3.1-1.0.0/spark-metrics-assembly-3.1-1.0.0.jar
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # BanzaiCloud Spark Metrics Setup (Cont)
# MAGIC 
# MAGIC #### Add initscript to cluster and restart cluster
# MAGIC Once the cluster restarts, you should be able to view the metrics from the pushgateway's /metrics endpoint as well as in Prometheus server.

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Prometheus Servelet Setup
# MAGIC 
# MAGIC Native support for Prometheus monitoring has been added to [Apache Spark 3.0](https://databricks.com/session_na20/native-support-of-prometheus-monitoring-in-apache-spark-3-0). Once configured, the driver will present a Prometheus scrape target which may be read by Prometheus server. Because we use Kubernetes on GCP to host Databricks Spark, we will utilize the Driver Proxy to expose the scrape target to Prometheus server.
# MAGIC 
# MAGIC #### First, make a note of the driver port. You'll need this when setting up the scrape target.

# COMMAND ----------

spark.conf.get("spark.ui.port")

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Prometheus Servelet Setup (Cont)
# MAGIC 
# MAGIC ####Next, create an init script as shown below:

# COMMAND ----------

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

# MAGIC %md
# MAGIC # Spark Prometheus Servelet Setup (Cont)
# MAGIC #### Add init script to cluster and restart cluster
# MAGIC Once the cluster is running, we'll instruct Prometheus to scrape the cluster:
# MAGIC 
# MAGIC 1) Generate a personal access token as shown [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token)
# MAGIC 
# MAGIC 2) Add the following to your prometheus_values.yaml
# MAGIC 
# MAGIC ```
# MAGIC extraScrapeConfigs: |
# MAGIC    - job_name: '<cluster_name>'
# MAGIC      metrics_path: /driver-proxy-api/o/0/<cluster_id>/<spark_ui_port>/metrics/prometheus/
# MAGIC      static_configs:
# MAGIC        - targets:
# MAGIC          - <workspace_url - i.e. 3888031910422875.5.gcp.databricks.com>
# MAGIC      authorization:
# MAGIC      # Sets the authentication type of the request.
# MAGIC        type: Bearer
# MAGIC        credentials: <personal access token>
# MAGIC ```
# MAGIC 
# MAGIC 3) Update your helm installation: ``` helm upgrade <installation_name - i.e. dlyle-prometheus> -f prometheus_values.yaml -n <namespace - i.e. prometheus-monitoring> --create-namespace prometheus-community/prometheus ```
# MAGIC 
# MAGIC 4) Prometheus server has a sidecar container that _should_ automatically update the config. If you're like me and you don't like waiting, you can force a restart: ``` kubectl rollout restart deployment <installation_name> -n <namespace> ```
# MAGIC 
# MAGIC At this point, you should be able to see your cluster as a scrape target in Prometheus server.
