# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Records from Delta Change Data Feed
# MAGIC
# MAGIC We will demonstrate an end-to-end of how you can propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
# MAGIC
# MAGIC
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed.We get IOT data using Structured Streaming and save it on the Bronze layer
# MAGIC
# MAGIC ### Silver Table
# MAGIC Deduplicate streaming data and using a Merge statement to write unique records to the Silver layer
# MAGIC
# MAGIC ### Gold Table
# MAGIC Propagate Updates and Deletes to the Gold Layer using Merge statement

# COMMAND ----------

# MAGIC %md
# MAGIC Enable CDF using Spark conf setting in a notebook or on a cluster will ensure it's used on all newly created Delta tables in that scope.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

# MAGIC %md Create the bronze table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS example.bronze_layer
# MAGIC   (messageId string,deviceId INT,temperature DOUBLE,rpm DOUBLE,angle DOUBLE,timestamp TIMESTAMP) 
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED example.bronze_layer

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE example.bronze_layer
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md Create and start the stream.
# MAGIC
# MAGIC For this example, we will:
# MAGIC * Use continuous processing (<b>trigger(processingTime='0 seconds')<b>)

# COMMAND ----------

import pyspark.sql.types as T
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# Event Hubs configuration
EH_NAMESPACE                    = ""
EH_NAME                         = ""

EH_CONN_SHARED_ACCESS_KEY_NAME  = "iothubowner"
EH_CONN_SHARED_ACCESS_KEY_VALUE = ""

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

EH_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
}

# PAYLOAD SCHEMA
schema = """messageId string,deviceId int, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"""

(spark.readStream.format("kafka")
        .options(**EH_OPTIONS)                                                         
        .load()
        .withColumn('body', F.from_json(F.col('value').cast('string'), schema))
        .withColumn('timestamp', F.current_timestamp())
        .select(
            F.col("body.messageId").alias("messageID"),
            F.col("body.deviceId").alias("deviceId"),
            F.col("body.temperature").alias("temperature"),
            F.col("body.rpm").alias("rpm"),
            F.col("body.angle").alias("angle"),
            F.col("timestamp").alias("timestamp"),
        )
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/FileStore/bronze_layer_checkpoint")
        .trigger(processingTime='0 seconds')
        .table("example.bronze_layer")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.bronze_layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('example.bronze_layer',0)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY example.bronze_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Data with Delta Lake
# MAGIC
# MAGIC Here we define upsert logic into the silver table using a streaming read against the bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS example.silver_layer
# MAGIC   (messageId string,deviceId INT,temperature DOUBLE,rpm DOUBLE,angle DOUBLE,timestamp TIMESTAMP) 
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO example.silver_layer s
        USING updates u
        ON s.deviceId = u.deviceId
        WHEN NOT MATCHED
            THEN INSERT *
    """)

# COMMAND ----------

(spark.readStream
                   .option("skipChangeCommits", "true")
                   .table("example.bronze_layer")
                   .withWatermark("timestamp", "10 seconds")
                   .dropDuplicates(["deviceId"])
                   .writeStream
                   .foreachBatch(upsert_to_silver)
                   .outputMode("update")
                   .option("checkpointLocation", f"dbfs:/FileStore/silver_layer_checkpoint4")
                   .trigger(processingTime='0 seconds')
                   .start())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.silver_layer

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.silver_layer

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/example.db/silver/

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY example.silver_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the Change Data Feed Batch from table_changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- version as ints or longs e.g. changes from version 0 to 10
# MAGIC -- SELECT * FROM table_changes('example.silver_layer', 0, 15)
# MAGIC
# MAGIC -- timestamp as string formatted timestamps
# MAGIC -- SELECT * FROM table_changes('example.silver_layer', '2024-02-11T09:53:15.000+00:00', '2024-02-11T10:07:12.000+00:00')
# MAGIC
# MAGIC -- -- providing only the startingVersion/timestamp
# MAGIC SELECT * FROM table_changes('example.silver_layer', 0)

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion", 4)
                         .table("example.silver_layer"))

silver_stream_df.filter(F.col("_change_type").isin(["update_postimage", "insert"])).selectExpr("*").display()

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion",1,15)
                         .table("example.silver_layer"))

silver_stream_df.filter(F.col("_change_type").isin(["update_preimage"])).selectExpr("*").display()

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion",1,9)
                         .table("example.silver_layer"))

silver_stream_df.filter(F.col("_change_type").isin(["delete"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO example.silver_layer VALUES (12,12,12,12,12,'2024-02-01T11:27:23.184+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE example.silver_layer
# MAGIC SET temperature=54 WHERE deviceId=12

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM example.silver_layer
# MAGIC WHERE deviceId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_table_latest_version as
# MAGIC SELECT _change_type,
# MAGIC         deviceId,
# MAGIC         min(temperature) as min_temperature,
# MAGIC         mean(rpm) as avg_rpm,
# MAGIC         max(angle) as max_angle 
# MAGIC FROM 
# MAGIC         (SELECT *, row_number() over (partition by deviceId order by _commit_version desc) as RN
# MAGIC         FROM table_changes('example.silver_layer', 0, 15)
# MAGIC         WHERE _change_type in ("update_postimage", "insert"))
# MAGIC WHERE RN=1
# MAGIC GROUP BY deviceId,_change_type
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_table_latest_version

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE spark_catalog.example.gold_layer(
# MAGIC   deviceId INT,
# MAGIC   min_temperature DOUBLE,
# MAGIC   avg_rpm DOUBLE,
# MAGIC   max_angle DOUBLE)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO example.gold_layer g 
# MAGIC USING silver_table_latest_version s ON s.deviceId = g.deviceId
# MAGIC WHEN MATCHED AND s._change_type='update_postimage' 
# MAGIC THEN UPDATE SET min_temperature = s.min_temperature
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deletes AS (
# MAGIC   SELECT distinct deviceId
# MAGIC   FROM table_changes("example.silver_layer", 0)
# MAGIC   WHERE _change_type='delete'
# MAGIC )
# MAGIC
# MAGIC MERGE INTO example.gold_layer g
# MAGIC USING deletes d
# MAGIC ON d.deviceId=g.deviceId
# MAGIC WHEN MATCHED
# MAGIC   THEN DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE
# MAGIC   example.temp_silver
# MAGIC AS SELECT messageId,
# MAGIC           deviceId,
# MAGIC           temperature,
# MAGIC           rpm,
# MAGIC           angle,
# MAGIC           timestamp,
# MAGIC           _change_type as operation_type,
# MAGIC           _commit_version as c_version,
# MAGIC           _commit_timestamp as c_time
# MAGIC FROM table_changes("example.silver_layer", 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.temp_silver
