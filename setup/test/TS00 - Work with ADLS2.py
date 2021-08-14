# Databricks notebook source
df = spark.read.parquet("/mnt/datalake/raw/silver/bike/sales")
df.show()
