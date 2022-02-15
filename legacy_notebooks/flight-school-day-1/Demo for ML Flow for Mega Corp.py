# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC <img src="/files/flight/Proposed_Architecture.png?raw=true" width=1000/>

# COMMAND ----------

# MAGIC %sh
# MAGIC mlflow --version

# COMMAND ----------

# Note that we have factored out the setup processing into a different notebook, which we call here.
# As a flight school student, you will probably want to look at the setup notebook.  
# Even though you'll want to look at it, we separated it out in order to demonstrate a best practice... 
# ... you can use this technique to keep your demos shorter, and avoid boring your audience with housekeeping.
# In addition, you can save demo time by running this initial setup command before you begin your demo.

# This cell should run in a few minutes or less

team_name = dbutils.widgets.get("team_name")

setup_responses = dbutils.notebook.run("./includes/flight_school_assignment_3_setup", 0, {"team_name": team_name}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

# COMMAND ----------

# Let's set the default database name so we don't have to specify it on every query

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
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow functionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# These are the Spark ML library functions we'll be using

from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 

import mlflow
from mlflow import spark as mlflow_spark # renamed to prevent collisions when doing spark.sql

import time


# COMMAND ----------

# MAGIC %md
# MAGIC ###PART 1... 
# MAGIC 
# MAGIC __*Developing*__ a Machine Learning model with the help of __MLflow Tracking__

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our current readings (labeled version)
# MAGIC -- This is the data we'll be using to train our Machine Learning Model
# MAGIC 
# MAGIC -- Note that device_operational_status is what our ML algorithm will try to predict.  This is called the "label" in ML parlance.  
# MAGIC -- We need it in this test file, because that's what we are using for "training" data for the ML model.
# MAGIC 
# MAGIC -- We'll use device_type, device_id, reading_1, reading_2, and reading_3 as inputs to examine to make our prediction.
# MAGIC -- These columns are called "features" in ML parlance.
# MAGIC 
# MAGIC -- The type of ML model we will build is called a "Decision Tree."  It can examine the features, and categorize each row 
# MAGIC -- into one of the "label" categories.
# MAGIC 
# MAGIC SELECT * FROM current_readings_labeled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a closer look at the number of values for each column
# MAGIC -- There are 7 different operational_status values.  That's what we are trying to predict when we process an unlabeled row.
# MAGIC -- There are only 2 device_type values, and only 18 device_id values.  In our model, those will be "categorical" features.
# MAGIC -- There are lots of distinct values for reading_1, reading_2, and reading_3.  In our model, those will be "continuous" features.
# MAGIC 
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT device_operational_status) AS distinct_operational_statuses,
# MAGIC   COUNT(DISTINCT device_type) AS distinct_device_types,
# MAGIC   COUNT(DISTINCT device_id) AS distinct_device_ids,
# MAGIC   COUNT(DISTINCT reading_1) AS distinct_reading_1s,
# MAGIC   COUNT(DISTINCT reading_2) AS distinct_reading_2s,
# MAGIC   COUNT(DISTINCT reading_3) AS distinct_reading_3s
# MAGIC FROM current_readings_labeled

# COMMAND ----------

# MAGIC %md
# MAGIC #### Building a Model with MLflow Tracking
# MAGIC 
# MAGIC The cell below creates a Python function that builds, tests, and trains a Decision Tree model.  
# MAGIC 
# MAGIC We made it a function so that you can easily call it many times with different parameters.  This is a convenient way to create many different __Runs__ of an experiment, and will help us show the value of MLflow Tracking.
# MAGIC 
# MAGIC Read through the code in the cell below, and notice how we use MLflow Tracking in several different ways:
# MAGIC   
# MAGIC - First, we __initiate__ MLflow Tracking like this: 
# MAGIC 
# MAGIC ```with mlflow.start_run() as run:```
# MAGIC 
# MAGIC Then we illustrate several things we can do with Tracking:
# MAGIC 
# MAGIC - __Tags__ let us assign free-form name-value pairs to be associated with the run.  
# MAGIC 
# MAGIC - __Parameters__ let us name and record single values for a run.  
# MAGIC 
# MAGIC - __Metrics__ also let us name and record single numeric values for a run.  We can optionally record *multiple* values under a single name.
# MAGIC 
# MAGIC - Finally, we will log the __Model__ itself.
# MAGIC 
# MAGIC Notice the parameters that the function below accepts:
# MAGIC 
# MAGIC - __p_max_depth__ is used to specify the maximum depth of the decision tree that will be generated.  You can vary this parameter to tune the accuracy of your model
# MAGIC 
# MAGIC - __p_owner__ is the "value" portion of a Tag we have defined.  You can put any string value into this parameter.

# COMMAND ----------

# Call this function to train and test the Decision Tree model
# We put this code into a function so you can easily create multiple runs by calling it with different parameters
# It uses MLflow to track runs
# Remember to run this cell before calling it.  You must also run this cell every time you change something in it.  Otherwise, your changes will not be "seen."

def training_run(p_max_depth = 2, p_owner = "default", p_run_name="team-2-fs-runs") :

  with mlflow.start_run(run_name=p_run_name) as run:    
    # Start a timer to get overall elapsed time for this function
    overall_start_time = time.time()
    step_elapsed_time = {}
    # TO DO... use the mlflow api to log a Tag named "Owner" and set the value to p_owner
    mlflow.log_param("owner", p_owner)
    
    # Log the p_max_depth parameter in MLflow
     
    mlflow.log_param("Maximum Depth", p_max_depth)
    
    # STEP 1: Read in the raw data to use for training
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
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
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    #
    # TO DO... use the mlflow API to log a Metric named "Step Elapsed Time" and set the value to the elapsed_time calculated above
    # NOTE: this will be a multi-step metric that shows the elapsed time for each step in this function.
    #       Set this call to be step 1
    #     mlflow.log_metric("Step Elapsed Time - Step 1", elapsed_time)
    step_elapsed_time['Step 1'] = elapsed_time
    
    # STEP 2: Index the Categorical data so the Decision Tree can use it
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    # Create a numerical index of device_type values (it's a category, but Decision Trees don't need OneHotEncoding)
    device_type_indexer = StringIndexer(inputCol="device_type", outputCol="device_type_index")
    df_raw_data = device_type_indexer.fit(df_raw_data).transform(df_raw_data)

    # Create a numerical index of device_id values (it's a category, but Decision Trees don't need OneHotEncoding)
    device_id_indexer = StringIndexer(inputCol="device_id", outputCol="device_id_index")
    df_raw_data = device_id_indexer.fit(df_raw_data).transform(df_raw_data)

    # Create a numerical index of label values (device status) 
    label_indexer = StringIndexer(inputCol="label", outputCol="label_index")
    df_raw_data = label_indexer.fit(df_raw_data).transform(df_raw_data)
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #       Set this call to be step 2
#     mlflow.log_metric("Step Elapsed Time - Step 2", elapsed_time)
    step_elapsed_time['Step 2'] = elapsed_time
    
    # STEP 3: create a dataframe with the indexed data ready to be assembled
    # We'll use an MLflow metric to log the time taken in each step 
    
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    # Populated df_raw_data with the all-numeric values
    df_raw_data.createOrReplaceTempView("vw_raw_data")
    df_raw_data = spark.sql("""
    SELECT 
      label_index AS label, 
      device_type_index AS device_type,
      device_id_index AS device_id,
      reading_1,
      reading_2,
      reading_3
    FROM vw_raw_data
    """)
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #       Set this call to be step 3
#     mlflow.log_metric("Step Elapsed Time - Step 3", elapsed_time)
    step_elapsed_time['Step 3'] = elapsed_time
   
    # STEP 4: Assemble the data into label and features columns
    
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    assembler = VectorAssembler( 
    inputCols=["device_type", "device_id", "reading_1", "reading_2", "reading_3"], 
    outputCol="features")

    df_assembled_data = assembler.transform(df_raw_data).select("label", "features")
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #       Set this call to be step 4
#     mlflow.log_metric("Step Elapsed Time - Step 4", elapsed_time)
    step_elapsed_time['Step 4'] = elapsed_time
    
    # STEP 5: Randomly split data into training and test sets. Set seed for reproducibility
    
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    (training_data, test_data) = df_assembled_data.randomSplit([0.7, 0.3], seed=100)
    
    # We'll use an MLflow metric to log number of training and test rows
    mlflow.log_metric("Training Data Rows", training_data.count())
    mlflow.log_metric("Test Data Rows", test_data.count())
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Set this call to be step 5
    # mlflow.log_metric("Step Elapsed Time - Step 5", elapsed_time)
    step_elapsed_time['Step 5'] = elapsed_time
    
    # STEP 6: Train the model
    start_time = time.time()
    
    # Select the Decision Tree model type, and set its parameters
    dtClassifier = DecisionTreeClassifier(labelCol="label", featuresCol="features")
    dtClassifier.setMaxDepth(p_max_depth)
    dtClassifier.setMaxBins(20) # This is how Spark decides if a feature is categorical or continuous

    # Train the model
    model = dtClassifier.fit(training_data)
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #       Set this call to be step 6
    #     mlflow.log_metric("Step Elapsed Time - Step 6", elapsed_time)
    step_elapsed_time['Step 6'] = elapsed_time
    
    # STEP 7: Test the model
    
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    df_predictions = model.transform(test_data)
    print(test_data.limit(2))
    print(df_predictions.limit(20))
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #       Set this call to be step 7
    #     mlflow.log_metric("Step Elapsed Time - Step 7", elapsed_time)
    step_elapsed_time['Step 7'] = elapsed_time
    
    # STEP 8: Determine the model's accuracy
    
    # We'll use an MLflow metric to log the time taken in each step 
    start_time = time.time()
    
    evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction")
    accuracy = evaluator.evaluate(df_predictions, {evaluator.metricName: "accuracy"})
    
    # Log the model's accuracy in MLflow
    mlflow.log_metric("Accuracy", accuracy)
    
    # Log the model's feature importances in MLflow
    #
    # TO DO... use the mlflow API to log a Parameter named "Feature Importances" 
    # and set the value to a model attribute called model.featureImportances (cast to a string)
    #
    mlflow.log_param("Feature Importances", str(model.featureImportances))
    
    # We'll use an MLflow metric to log the time taken in each step 
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    #     mlflow.log_metric("Step Elapsed Time - Step 8", elapsed_time)
    step_elapsed_time['Step Elapsed Time'] = elapsed_time
    #     metrics = {"mse": 2500.00, "rmse": 50.00}
    #     mlflow.log_metrics(step_elapsed_time.values, step = [1,2,3,4,5,6,7,8])
    # We'll also use an MLflow metric to log overall time
    overall_end_time = time.time()
    overall_elapsed_time = overall_end_time - overall_start_time
    
    # Log the total elapsed time
    mlflow.log_metric("Overall Elapsed Time", overall_elapsed_time)
    print(test_data.schema)
    print(test_data.limit(5).toPandas().to_json(orient='split'))
    # Log the model itself
    #mlflow_spark.log_model(model, artifact_path="decision-tree-model", input_example=test_data.limit(5).toPandas().to_dict())    
    mlflow_spark.log_model(model, artifact_path="decision-tree-model")    
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC Now we're ready to do a few runs.
# MAGIC 
# MAGIC Before you begin, click "Experiment" in the upper right of the notebook.  This is our link to the MLflow UI.
# MAGIC 
# MAGIC If this is the first time you have run this notebook, you will see that no runs have been recorded yet: <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_runs_no_runs_yet_v2.PNG?raw=true" width=300/>
# MAGIC 
# MAGIC In the cell below, set the p_max_depth parameter (hint: try values between 1 and 10).  Set the p_owner value to some text string.  Run the cell several times, and change the parameters for each execution.

# COMMAND ----------

# Train and test a model.  Run this several times using different parameter values.  
# HINT: values between 1 and 10 work well for p_max_depth
# The called function returns a UUID for the Run.
 
x = [2,4,6,8]
for n in x:
  mlFlowOutput = training_run(p_max_depth=n, p_run_name="team-2-fs-run-" + str(n))
  print(mlFlowOutput)

# COMMAND ----------

# MAGIC  %md
# MAGIC  ### To Summarize
# MAGIC  
# MAGIC  We learned a lot about MLflow Tracking in this module.  We tracked:
# MAGIC  
# MAGIC  - Parameters
# MAGIC  - Metrics
# MAGIC  - Tags
# MAGIC  - The model itself
# MAGIC  
# MAGIC  
# MAGIC  Then we showed how to find a model, instantiate it, and run it to make predictions on a different data set.
# MAGIC  
# MAGIC  __*But we've just scratched the surface of what MLflow can do...*__
# MAGIC  
# MAGIC  To learn more, check out documentation and notebook examples for MLflow Models and MLflow Registry.

# COMMAND ----------

# MAGIC %md
# MAGIC ###PART 2...
# MAGIC 
# MAGIC __*Examining* the MLflow UI__
# MAGIC 
# MAGIC Now click "Experiment" again, and you'll see something like this.  Click the link that takes us to the top-level MLflow page: <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_runs_v2.png?raw=true" width=300/>
# MAGIC 
# MAGIC The __top-level page__ summarizes all our runs.  
# MAGIC 
# MAGIC Notice how our custom parameters and metrics are displayed.
# MAGIC 
# MAGIC Click on the "Accuracy" or "Overall Elapsed Time" columns to quickly create leaderboard views of your runs.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_summary_page.png?raw=true" width=1500/>
# MAGIC 
# MAGIC Now click on one of the runs to see a __detail page__.  Examine the page to see how our recorded data is shown.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_detail_page.png?raw=true" width=700/>
