# Databricks notebook source
## Load Data
parquetPath = "dbfs:/app/diabetics.parquet"
df = spark.read.parquet(parquetPath)

df = df.drop("readmitted")
df = df.dropna()
cols = df.columns

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from distutils.version import LooseVersion

## this function is necessary to transform categories that are of type string into index
#MORE ABOUT STRINGINDEXER:
#https://docs.databricks.com/applications/machine-learning/mllib/decision-trees.html
#https://docs.databricks.com/applications/machine-learning/mllib/binary-classification-mllib-pipelines.html

categoricalColumns = ["race","gender","age","admission_type_id","discharge_disposition_id","admission_source_id","diag_1","diag_2","diag_3","max_glu_serum","A1Cresult","metformin","repaglinide","nateglinide","chlorpropamide","glimepiride","glipizide","glyburide","tolbutamide","pioglitazone","rosiglitazone","acarbose","miglitol","troglitazone","tolazamide","insulin","change", "diabetesMed"]
stages = [] # stages in our Pipeline
for categoricalCol in categoricalColumns:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
    # Use OneHotEncoder to convert categorical variables into binary SparseVectors
    from pyspark.ml.feature import OneHotEncoderEstimator
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]
    
# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="in_readmitted", outputCol="label")
stages += [label_stringIdx]

# Transform all features into a vector using VectorAssembler
numericCols = ["time_in_hospital","num_lab_procedures","num_procedures","num_medications","number_outpatient","number_emergency","number_inpatient","number_diagnoses"]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

##Run the stages as a Pipeline. This puts the data through all of the feature transformations we described in a single call.
##more information:
#https://spark.apache.org/docs/2.2.0/ml-pipeline.html

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(df)
preppedDataDF = pipelineModel.transform(df)

## split data into training and test sets
(trainingData, testData) = preppedDataDF.randomSplit([0.7, 0.3])

# COMMAND ----------

parquetPath = "dbfs:/app/diabeticstest.parquet"
dfSample = testData.limit(100)
dfSample.write.format("parquet").option('path', parquetPath).mode("overwrite")
