# Databricks notebook source
## Load Data
parquetPath = "dbfs:/app/diabetics.parquet"
df = spark.read.parquet(parquetPath)
dfDiab = df.select("gender","diag_1","in_readmitted")

# COMMAND ----------

from pyspark.sql.functions import count, desc

dfDiabDiag = dfDiab.select("diag_1","in_readmitted")
dfDiabDiag = dfDiabDiag.groupBy("diag_1").agg(count("in_readmitted").alias("cntreadmitted"))
dfDiabDiag = dfDiabDiag.orderBy("diag_1")
display(dfDiabDiag)

# COMMAND ----------

dfDiabDiag = dfDiabDiag.orderBy(desc("cntreadmitted"))
dfDiabDiag = dfDiabDiag.limit(10)
display(dfDiabDiag)

# COMMAND ----------

dfDiabGen = dfDiab.select("gender","in_readmitted")
dfDiabGen = dfDiabGen.groupBy("gender").agg(count("in_readmitted").alias("cntreadmitted"))
display(dfDiabGen)
