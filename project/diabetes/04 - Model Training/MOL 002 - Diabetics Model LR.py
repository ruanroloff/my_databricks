# Databricks notebook source
### RUN FEATURE

# COMMAND ----------

# MAGIC %run "./MOL 001 - Diabetics Feature"

# COMMAND ----------

### LogisticRegression model ###
from pyspark.ml.classification import LogisticRegression

## Create LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=3)

## Train model with Training Data
lrModel = lr.fit(trainingData)

## Make predictions on test data using the transform() method.
## LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)

# COMMAND ----------

### ROC for training data
## The best cut-off has the highest true positive rate together with the lowest false positive rate. 
## source: http://spark.apache.org/docs/latest/mllib-evaluation-metrics.html
display(lrModel, preppedDataDF, "ROC")

# COMMAND ----------

#residuals versus fitted data
display(lrModel, preppedDataDF)

# COMMAND ----------

#predctions. show predictions based on testdata dataset
selected = predictions.select("label", "prediction", "probability", "age", "gender", "diabetesMed", "in_readmitted")
display(selected)

# COMMAND ----------

### Evaluate: Accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
lr_accuracy = evaluator.evaluate(predictions)
print("Accuracy of LogisticRegression is = %g"% (lr_accuracy))
