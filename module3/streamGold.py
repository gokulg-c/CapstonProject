# Databricks notebook source
import dlt

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##identify if any of the fraud callers are existing customers or any of the existing customers receiving calls from fraud numbers

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers stream data.",
  table_properties={
    "wetelco.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraudIdentification():
    stream_df = dlt.read("customer_stream_raw")
    customer_information_df = dlt.read("customer_info_silver")
    joined_stream_df = stream_df.join(
        customer_information_df,
        (stream_df.caller_number == customer_information_df.customer_phone) |
        (stream_df.receiver_number == customer_information_df.customer_phone),
        "inner"
    )
    return joined_stream_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##duration of call by suspected reason

# COMMAND ----------

from pyspark.sql.functions import *
@dlt.create_table(
  comment="duration of call by suspected region",
  table_properties={
    "wetelco.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def average_duration_by_reason():
    stream_df = dlt.read("customer_stream_raw")
    # Convert start_time and end_time to timestamp
    stream_df = stream_df.withColumn("start_time", from_utc_timestamp(unix_timestamp("start_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp"), "UTC"))
    stream_df = stream_df.withColumn("end_time", from_utc_timestamp(unix_timestamp("end_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp"), "UTC"))

    # Calculate duration in seconds
    stream_df = stream_df.withColumn("call_duration", (unix_timestamp("end_time") - unix_timestamp("start_time")))

    # Group by suspected_reason and calculate the average duration
    average_duration_by_reason = stream_df.groupBy("suspected_reason").agg(avg(col("call_duration")).alias("avg")) 
    return average_duration_by_reason

# COMMAND ----------

from pyspark.sql.functions import *
@dlt.create_table(
  comment="fraud_calls_by_location",
  table_properties={
    "wetelco.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraud_calls_by_location():
    stream_df = dlt.read("customer_stream_raw")
    fraud_calls_by_location = stream_df.groupBy("location") \
        .agg({"location": "count"}) \
        .withColumnRenamed("count(location)", "no_of_fraud_calls")
    return fraud_calls_by_location
