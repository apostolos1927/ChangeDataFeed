# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.table
def temp_silver_table():
  return spark.readStream.format("delta").table("example.temp_silver")

dlt.create_streaming_table("target_DLT_python")

dlt.apply_changes(
  target = "target_DLT_python",
  source = "temp_silver_table",
  keys = ["deviceId"],
  sequence_by = col("com_time"),
  apply_as_deletes = expr("operation_type = 'delete'"),
  except_column_list = ["rpm"],
  stored_as_scd_type = "2"
)
