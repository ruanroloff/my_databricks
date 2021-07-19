# Databricks notebook source
# MAGIC %run "/Users/ruanroloff@gmail.com/00 - Scripts/env/registerlib"

# COMMAND ----------

import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
from pyspark.ml import PipelineModel
deserializedPipeline = PipelineModel.deserializeFromBundle("jar:file:/tmp/mleap_python_model_export/pipeline-diabet_v1.zip")

parquetPath = "dbfs:/app/diabeticstest.parquet"
df = spark.read.parquet(parquetPath)
test_df = df
test_df.cache()
#display(test_df)

# COMMAND ----------

exampleResults = deserializedPipeline.transform(test_df)
display(exampleResults)
