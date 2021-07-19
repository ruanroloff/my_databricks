# Databricks notebook source
### RUN FEATURE

# COMMAND ----------

# MAGIC %run "/Users/ruanroloff@gmail.com/04 - Model Training/MOL 001 - Diabetics Feature"

# COMMAND ----------

##TRY ANOTHER TYPE OF MODEL
### DecisionTreeClassifier ###

from pyspark.ml.classification import DecisionTreeClassifier

# Create initial Decision Tree Model
##maxDepth : increse de tree branch
dtc = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=5)

# Train model with Training Data
dtcModel = dtc.fit(trainingData)

# Viewing the Tree
display(dtcModel)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
# Make predictions on test data using the Transformer.transform() method.
predictions = dtcModel.transform(testData)

# Evaluate model
evaluator = BinaryClassificationEvaluator()
dtree_accuracy = evaluator.evaluate(predictions)
print("Accuracy of DecisionTree is = %g"% (dtree_accuracy))

# COMMAND ----------

#predctions. show predictions based on testdata dataset
selected = predictions.select("label", "prediction", "probability", "age", "gender", "diabetesMed", "in_readmitted")
display(selected)
