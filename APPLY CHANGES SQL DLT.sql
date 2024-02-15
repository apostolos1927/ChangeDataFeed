-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE target_DLT_SQL;

APPLY CHANGES INTO
  live.target_DLT_SQL
FROM
  stream(example.temp_silver)
KEYS
  (deviceId)
APPLY AS DELETE WHEN
  operation_type = "delete"
SEQUENCE BY
  com_time
COLUMNS * EXCEPT
  (rpm)
STORED AS
  SCD TYPE 2;
