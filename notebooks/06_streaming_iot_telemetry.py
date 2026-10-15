# Databricks notebook source
# MAGIC %md
# MAGIC # IoT & Web Telemetry Lakehouse (Batch-Simulated Stream)
# MAGIC This script simulates real-time IoT device and web clickstream telemetry using robust windowed aggregations, 
# MAGIC fully compatible with serverless and constrained Databricks cluster runtimes.

# COMMAND:
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Generate a mock telemetry dataset simulating a stream of events
# (Using range to create 1,000 incoming telemetry records)
raw_stream_df = spark.range(1000).select(
    F.concat(F.lit("device_"), (F.col("id") % 5).cast(StringType())).alias("device_id"),
    F.when(F.col("id") % 3 == 0, F.lit("WARNING")).otherwise(F.lit("INFO")).alias("event_type"),
    (F.rand() * 100).alias("cpu_load"),
    (F.rand() * 60 + 40).alias("memory_usage"),
    F.current_timestamp().alias("event_time")
)

print(f"Generated {raw_stream_df.count()} raw telemetry event records.")

# COMMAND:
# 2. Compute Windowed Aggregations (Simulating Silver/Gold Real-Time Analytics)
windowed_telemetry_df = (
    raw_stream_df
    .groupBy(
        F.window(F.col("event_time"), "1 hour"),
        F.col("device_id")
    )
    .agg(
        F.avg("cpu_load").alias("avg_cpu_load"),
        F.max("memory_usage").alias("max_memory_usage"),
        F.count("*").alias("event_count")
    )
    .orderBy("device_id")
)

# COMMAND:
# 3. Save as a Registered Delta Table (Ready for AI/BI Dashboards)
windowed_telemetry_df.write.mode("overwrite").saveAsTable("gold_telemetry_summary")

print("Telemetry summary successfully written to Delta table: gold_telemetry_summary")

# COMMAND:
# 4. Display Live Results for Dashboard Verification
display(spark.sql("SELECT * FROM gold_telemetry_summary"))
