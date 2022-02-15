# Databricks notebook source
# This creates the "team_name" field displayed at the top of the notebook.

dbutils.widgets.text("team_name", "Enter your team's name");

# COMMAND ----------

team_name = dbutils.widgets.get("team_name")

setup_responses = dbutils.notebook.run("./includes/flight_school_assignment_3_setup", 0, {"team_name": team_name}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC 
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning life cycle, data scientists test many different models from various libraries with different hyperparameters.  Tracking these various results poses an organizational challenge.  In brief, storing experiments, results, models, supplementary artifacts, and code creates significant challenges.
# MAGIC 
# MAGIC MLflow Tracking is one of the three main components of MLflow.  It is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:<br><br>
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiment
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This demo will use Python, though the majority of MLflow functionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img src="/files/flight/Proposed_Architecture.png?raw=true" width=1000/>
# MAGIC 
# MAGIC ### Megacorp Power Plant Sensors
# MAGIC Here at Megacorp, you receive sensor data from Rectifiers and Transformers to monitor the operational status from these devices.
# MAGIC 
# MAGIC - **Q:** Given a labeled dataset, how can we train a model to predict the device operational status?
# MAGIC - **A:** Databricks and mlflow!
# MAGIC 
# MAGIC - **Q:** Can the model be exposed to systems external to Databricks?  
# MAGIC 
# MAGIC - **A:** Of course, lets use mlflow and model serving to accomplish this.
# MAGIC 
# MAGIC 
# MAGIC In this demo, we'll use a pre-loaded dataset, **current_reading_labeled**, to train a [DecisionTreeClassifier](https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html) to predict the operational status.
# MAGIC 
# MAGIC We'll introduce the concept of an machine learing [Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html) comprised of Transformers and Estimators to easily combine our data preparation and machine learning tasks in to a discrete unit of work for easy serving.
# MAGIC 
# MAGIC #### Main concepts in Pipelines
# MAGIC MLlib standardizes APIs for machine learning algorithms to make it easier to combine multiple algorithms into a single pipeline, or workflow. This section covers the key concepts introduced by the Pipelines API, where the pipeline concept is mostly inspired by the scikit-learn project.
# MAGIC 
# MAGIC **DataFrame**: This ML API uses DataFrame from Spark SQL as an ML dataset, which can hold a variety of data types. E.g., a DataFrame could have different columns storing text, feature vectors, true labels, and predictions.
# MAGIC 
# MAGIC **Transformer**: A Transformer is an algorithm which can transform one DataFrame into another DataFrame. E.g., an ML model is a Transformer which transforms a DataFrame with features into a DataFrame with predictions.
# MAGIC 
# MAGIC **Estimator**: An Estimator is an algorithm which can be fit on a DataFrame to produce a Transformer. E.g., a learning algorithm is an Estimator which trains on a DataFrame and produces a model.
# MAGIC 
# MAGIC **Pipeline**: A Pipeline chains multiple Transformers and Estimators together to specify an ML workflow.
# MAGIC 
# MAGIC **Parameter**: All Transformers and Estimators now share a common API for specifying parameters.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #### DataPrep
# MAGIC We'll have to convert the textural data to numbers using [StringIndexer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StringIndexer.html)(transformer) and use a [VectorAssembler](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.ml.feature.VectorAssembler.html)(transformer) to combine the features ("device_type", "device_id", "reading_1", "reading_2", "reading_3") into a single vector for use by the DecisionTreeClassifier (estimator).
# MAGIC 
# MAGIC We also have to randomly split the dataset into two parts- training and test data. Fortunately, Dataframes make this easy.

# COMMAND ----------

# MAGIC %sql
# MAGIC --lets get some data
# MAGIC SELECT * FROM current_readings_labeled

# COMMAND ----------

 df_raw_data = spark.sql("""
      SELECT 
        device_type,
        device_operational_status AS label,
        device_id,
        reading_1,
        reading_2,
        reading_3
      FROM current_readings_labeled
    """)
(training_data, test_data) = df_raw_data.randomSplit([0.7, 0.3], seed=100)

# COMMAND ----------

from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 
from pyspark.ml import Pipeline
import mlflow
from mlflow import spark as mlflow_spark # renamed to prevent collisions when doing spark.sql

import time

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Creating the Pipeline
# MAGIC We'll create a pipeline consisting of five steps:
# MAGIC - Conversion of device_type to a numeric value
# MAGIC - Conversion of device_id to a numeric value
# MAGIC - Conversion of device_operational_status (now called label) to a numeric value
# MAGIC - Vectorization of the feature set (device_type, device_id, reading_1, reading_2, reading_3)
# MAGIC - Classification of the feature set using the DecisionTreeClassifier
# MAGIC 
# MAGIC Notice that the pipeline is a single Estimator, so we can use the Estimator fit method to return a trained model which is realized as a Transformer. We can use the Transformer transform method to make predictions on the test data.

# COMMAND ----------

# First we're going to create a pipeline for our model 

device_type_indexer = StringIndexer(inputCol="device_type", outputCol="device_type_index")
device_id_indexer = StringIndexer(inputCol="device_id", outputCol="device_id_index")
label_indexer = StringIndexer(inputCol="label", outputCol="label_index")

assembler = VectorAssembler( 
    inputCols=["device_type_index", "device_id_index", "reading_1", "reading_2", "reading_3"], 
    outputCol="features")

dtClassifier = DecisionTreeClassifier(labelCol="label_index", featuresCol="features", predictionCol="prediction_label")
dtClassifier.setMaxBins(20)

pipeline = Pipeline(stages=[device_type_indexer, device_id_indexer, label_indexer, assembler, dtClassifier])


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Training Runs
# MAGIC Once the pipeline is created, we can run a series of experiments to determine which maximum depth produces the model which best fits the the test data. In the below function, we create a mlflow run, log the parameters, train the model, score the model and evaluate the accuracy of the model.
# MAGIC 
# MAGIC Notice the parameters:
# MAGIC - **p_max_depth** - the depth of the Decision Tree
# MAGIC - **p_owner** - who is running the experiments.
# MAGIC - **p_run_name** - the name of this test run
# MAGIC - **pipeline** - the pipeline to run
# MAGIC - **predictionCol** - normally this would be called 'prediction', but sometimes it is advantageous to change it. We'll see why later.

# COMMAND ----------

# Next, let's create a function to do repeated training runs and log them with mlflow
def training_run(p_max_depth = 2, p_owner = "default", p_run_name = "team-2-fs-runs", 
                 pipeline = Pipeline(), predictionCol='prediction') :
    
    with mlflow.start_run(run_name=p_run_name) as run:    
        
        mlflow.log_param("owner", p_owner)
        mlflow.log_param("Maximum Depth", p_max_depth)
        
        dtClassifier.setMaxDepth(p_max_depth)
        dtClassifier.setPredictionCol(predictionCol)
        
        pipelineModel = pipeline.fit(training_data)
        predDf = pipelineModel.transform(test_data)
        
        evaluator = MulticlassClassificationEvaluator(
        labelCol="label_index", predictionCol=predictionCol)       
        accuracy = evaluator.evaluate(predDf, {evaluator.metricName: "accuracy"})
        mlflow.log_metric("Accuracy", accuracy)
        
        mlflow_spark.log_model(pipelineModel, artifact_path="decision-tree-model", input_example=test_data.limit(4).toPandas()) 
        
        return run.info.run_uuid

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Run the Experiments
# MAGIC Below we will call several training runs at various depths. Each expirement will be logged by mlflow and will include example Input Data to make our lives easier should we decide to serve the model later.

# COMMAND ----------

# Call the function with different DT depths
x = [2,4,6,8]
for n in x:
    mlFlowOutput = training_run(p_max_depth=n,p_owner="david.lyle", p_run_name="predictor-numeric-output" + str(n), pipeline=pipeline)
    print(mlFlowOutput)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Evaulate the Results
# MAGIC I'd like to look at the output of the most accurate training run, so I'll select it from 

# COMMAND ----------

# Let's load the experiment... 
# If this were *really* another notebook, I'd have to obtain the Experiment ID from the MLflow page.  
# But since we are in the original notebook, I can get it as a default value

df_client = spark.read.format("mlflow-experiment").load()
df_client.createOrReplaceTempView("vw_client")

# COMMAND ----------

# Let's query the MLflow data in a way that shows us the most accurate model in the first row
# This is possible because we logged accuracy as a metric using MLflow
# Then we can grab the run_id to get a handle to the model itself

df_model_selector = spark.sql("""
    SELECT 
        experiment_id,
        run_id,
        end_time,
        metrics.Accuracy as accuracy,
        CONCAT(artifact_uri,'/decision-tree-model') as artifact_uri        
    FROM vw_client WHERE status == 'FINISHED'
        AND params.owner == 'david.lyle'  
        AND tags['mlflow.runName'] like '%numeric%'
    ORDER BY accuracy desc""")

display(df_model_selector)

# COMMAND ----------

#Load the model
pipelineModel = mlflow_spark.load_model(df_model_selector.first()[4])


# COMMAND ----------

outDf = pipelineModel.transform(test_data)
display(outDf.select('device_type', 'prediction', 'reading_1', 'reading_2', 'reading_3'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A Good Start
# MAGIC 
# MAGIC We now have a model that takes in device_type and readings and predicts device operational status. We could use this in production, but wouldn't it be better if the prediction were a word?
# MAGIC 
# MAGIC Let's see how we can extend the pipeline to change the prediction index back to a string- to do so, we'll use an IndexToString transformer.

# COMMAND ----------

# That worked well, but I'd like words please
# Set up a IndexToString to change the prediction back to a value
label_indexer_model = label_indexer.fit(training_data)
its = IndexToString(inputCol='prediction_label', outputCol='prediction',labels=label_indexer_model.labels)
#Notice- a pipeline is just a estimator, I can use it to create another pipeline with the IndexToString transformer
new_pipeline = Pipeline(stages=[pipeline, its])

# COMMAND ----------

# Let's evaluate it
dtClassifier.setPredictionCol('prediction_label')
new_pipeline_model = new_pipeline.fit(training_data)
newPredictions = new_pipeline_model.transform(test_data)
display(newPredictions.select('device_type', 'prediction', 'reading_1', 'reading_2', 'reading_3'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Success!
# MAGIC Looks like we now have a pipeline that will take in emitted sensor data and return a human-readable prediction. Now all that is left is to tune it and productionalize it. 
# MAGIC To tune it, we'll use the same training_run function we used on the prior pipeline. Remember how we passed the pipeline as a parameter- that's why.

# COMMAND ----------

# Call the new pipeline with different DT depths
x = [2,4,6,8]
for n in x:
    mlFlowOutput = training_run(p_max_depth=n,p_owner="david.lyle", p_run_name="predictor-text-output" + str(n), 
                                pipeline=new_pipeline, predictionCol="prediction_label")
    print(mlFlowOutput)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Model Serving
# MAGIC To serve this model, we'll use the Databricks Model Serving preview. This is an easy way to expose the model as a restful endpoint so other services can access it.
# MAGIC 
# MAGIC It works like this:
# MAGIC 
# MAGIC - Register the Model you'd like to serve.
# MAGIC - Select Enable-Serving from the Registered Model's page.
# MAGIC 
# MAGIC <div><img src="https://docs.databricks.com/_images/enable-serving.png" style="height: 400px; margin: 20px"/></div>
# MAGIC Let's see how it works.
