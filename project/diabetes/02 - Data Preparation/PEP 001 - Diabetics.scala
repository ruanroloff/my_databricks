// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val schema = StructType(
  List(
    StructField("encounter_id", DoubleType, false),
    StructField("patient_nbr", StringType, false),
    StructField("race", StringType, true),
    StructField("gender", StringType, true),
    StructField("age", StringType, true),
    StructField("weight", StringType, true),
    StructField("admission_type_id", StringType, true),
    StructField("discharge_disposition_id", StringType, true),
    StructField("admission_source_id", StringType, true),
    StructField("time_in_hospital", DoubleType, false),
    StructField("payer_code", StringType, true),
    StructField("medical_specialty", StringType, true),
    StructField("num_lab_procedures", DoubleType, false),
    StructField("num_procedures", DoubleType, false),
    StructField("num_medications", DoubleType, false),
    StructField("number_outpatient", DoubleType, false),
    StructField("number_emergency", DoubleType, false),
    StructField("number_inpatient", DoubleType, false),
    StructField("diag_1", StringType, true),
    StructField("diag_2", StringType, true),
    StructField("diag_3", StringType, true),
    StructField("number_diagnoses", DoubleType, false),
    StructField("max_glu_serum", StringType, true),
    StructField("A1Cresult", StringType, true),
    StructField("metformin", StringType, true),
    StructField("repaglinide", StringType, true),
    StructField("nateglinide", StringType, true),
    StructField("chlorpropamide", StringType, true),
    StructField("glimepiride", StringType, true),
    StructField("acetohexamide", StringType, true),
    StructField("glipizide", StringType, true),
    StructField("glyburide", StringType, true),
    StructField("tolbutamide", StringType, true),
    StructField("pioglitazone", StringType, true),
    StructField("rosiglitazone", StringType, true),
    StructField("acarbose", StringType, true),
    StructField("miglitol", StringType, true),
    StructField("troglitazone", StringType, true),
    StructField("tolazamide", StringType, true),
    StructField("examide", StringType, true),
    StructField("citoglipton", StringType, true),
    StructField("insulin", StringType, true),
    StructField("glyburide-metformin", StringType, true),
    StructField("glipizide-metformin", StringType, true),
    StructField("glimepiride-pioglitazone", StringType, true),
    StructField("metformin-rosiglitazone", StringType, true),
    StructField("metformin-pioglitazone", StringType, true),
    StructField("change", StringType, true),
    StructField("diabetesMed", StringType, true),
    StructField("readmitted", StringType, false)
  )
)


// fields diag_1, diag_2, diag_3 = DIAGONIS CID9 AND CID10
// although they are the same category, they are in different versions
// MORE INFORMATION: https://www.who.int/classifications/icd/en/#

var filePath = "dbfs:/tmp/diabetic_data.csv"
var parquetPath = "dbfs:/app/diabetics.parquet"

val df = spark.read.option("header", "true").schema(schema).csv(filePath)

// exclude non related variables
val dfDiab = df.drop("encounter_id","patient_nbr", "weight", "payer_code","medical_specialty") 
  .drop("acetohexamide","examide", "citoglipton","glyburide-metformin","glipizide-metformin","glimepiride-pioglitazone","metformin-rosiglitazone","metformin-pioglitazone")
  .withColumn("in_readmitted", when(col("readmitted") === "<30", "YES").otherwise("NO"))
  .filter(col("gender") =!= "Unknown/Invalid")
  .filter(col("diag_1") =!= "?")
  .filter(col("diag_2") =!= "?")
  .filter(col("diag_3") =!= "?")

//dfDiab.show()
dfDiab.write.format("parquet").option("path", parquetPath).mode("overwrite")
