# Databricks notebook source
##Only works on premium edtion
#dbutils.notebook.run("notebook_1", 60)

# COMMAND ----------

# MAGIC %run "/Users/ruanroloff@gmail.com/00 - Scripts/env/registerlib"

# COMMAND ----------

# MAGIC %run "/Users/ruanroloff@gmail.com/04 - Model Training/MOL 002 - Diabetics Model LR"

# COMMAND ----------

import mlflow
import mlflow.spark

with mlflow.start_run():
  lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=3)
  lrModel = lr.fit(trainingData)
  test_metric = evaluator.evaluate(lrModel.transform(testData))
  mlflow.log_param("param_maxIter", 3)
  mlflow.log_metric('test_' + evaluator.getMetricName(), test_metric) # Logs additional metrics
  mlflow.spark.log_model(spark_model=lrModel, artifact_path='EXP_00TEST') # Logs the best model
  mlflow.end_run()
