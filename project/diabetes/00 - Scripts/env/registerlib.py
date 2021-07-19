# Databricks notebook source
##INSTALL MLFLOW


# COMMAND ----------

dbutils.library.installPyPI("mlflow")

# COMMAND ----------

##INSTALL MLEAP
### step 01 - install lib on cluster: mleap:mleap-spark_2.11:0.16.0
### step 02 - install pip

# COMMAND ----------

dbutils.library.installPyPI("mleap")

# COMMAND ----------

#dbutils.library.restartPython()
