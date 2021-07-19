-- Databricks notebook source
DROP TABLE IF EXISTS diabetic;

CREATE TABLE diabetic
	USING csv
	OPTIONS (path "dbfs:/tmp/diabetic_data.csv", header "true");
