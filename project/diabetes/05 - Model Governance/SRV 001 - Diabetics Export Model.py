# Databricks notebook source
# MAGIC %run "diabet/00 - Scripts/env/registerlib"

# COMMAND ----------

# MAGIC %run "/Users/ruanroloff@gmail.com/04 - Model Training/MOL 002 - Diabetics Model LR"

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -rf /tmp/mleap_python_model_export
# MAGIC mkdir /tmp/mleap_python_model_export

# COMMAND ----------

### EXPORT MODEL
#dbutils.library.installPyPI("mleap")

import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer

## For Supported Features Transformer and Model
## GOTO : https://mleap-docs.combust.ml/core-concepts/transformers/support.html
lrModel.serializeToBundle("jar:file:/tmp/mleap_python_model_export/pipeline-diabet_v1.zip", predictions)
