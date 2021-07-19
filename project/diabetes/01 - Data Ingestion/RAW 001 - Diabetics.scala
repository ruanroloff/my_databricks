// Databricks notebook source
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

// download dataset
val tmpFile = new File("/tmp/dataset_diabetes.zip")
FileUtils.copyURLToFile(new URL("https://archive.ics.uci.edu/ml/machine-learning-databases/00296/dataset_diabetes.zip"), tmpFile)

// COMMAND ----------

// MAGIC %sh 
// MAGIC unzip -o /tmp/dataset_diabetes.zip -d /tmp/data/

// COMMAND ----------

// unzip and copy the file to databricks filesystem
dbutils.fs.rm("dbfs:/tmp/diabetic_data.csv")  
dbutils.fs.mv("file:/tmp/data/dataset_diabetes/diabetic_data.csv", "dbfs:/tmp/diabetic_data.csv")  
