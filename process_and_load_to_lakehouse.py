# Databricks notebook source
# Read data from delta table
df = spark.read.format("delta").load("path_to_delta_table")

# Write data to silver as merge statement
df.write.format("delta").mode("overwrite").save("path_to_silver_table")
