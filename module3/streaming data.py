# Databricks notebook source
# MAGIC %md
# MAGIC ##Streaming Data

# COMMAND ----------

container_name = "wetelcostreams"
account_name = "adlsstoragedata01"
storage_account_key = "tBwtMqWlyr9ToC74Jxtq1UrA9aFi8fugoJo2SaKHxbwQnSIimMs6QjLW/Xw2Ujpk6M/wb9F9BXeB+AStk6vtGQ=="
dbutils.fs.mount(
    source="wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
    mount_point="/mnt/wetelcostreams",
    extra_configs={"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
)

# COMMAND ----------

container_name = "mytelco"
account_name = "capstoneproject045"
storage_account_key = "YomsEYQH5ict2ov6Ld2NoBbxT3eFhjIQDAWGgmSxW2eHiD4mSu48DStIIKCqqvylq5fiRupFU1xv+ASt3UQakA=="
dbutils.fs.mount(
    source="wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
    mount_point="/mnt/wetelcoschema",
    extra_configs={"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
)

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers stream data.",
  table_properties={
    "WeEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customer_stream_raw():
    """
    Load and create a Delta Lake table for raw customer streaming data.

    This function reads streaming data in Parquet format from a specified location
    and creates a Delta Lake table for the raw customer streaming data.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer streaming data.
    """
    # reading the streaming data
    stream_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", "/mnt/wetelcoschema/outputschema") \
        .option("cloudFiles.schemaEvolutionMode","rescue") \
        .load("/mnt/wetelcostreams/*.parquet")
    column_names = stream_df.columns
    for column_name in column_names:
    stream_df = stream_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))
# Assuming streaming_df is your Spark DataFrame
    stream_df = stream_df.toDF(*[col.lower() for col in stream_df.columns])
    return stream_df

# COMMAND ----------

# customer_information_df = spark.read.parquet("/mnt/basedata/SilverLayerData/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##identify if any of the fraud callers are existing customers or any of the existing customers receiving calls from fraud numbers

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers stream data.",
  table_properties={
    "WeEnsure_delta.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraudIdentification():
    stream_df = dlt.read("customer_stream_raw")
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
    "WeEnsure_delta.quality": "gold",
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
    average_duration_by_reason = stream_df.groupBy("suspected_reason").agg({"call_duration": "avg"})    
    return average_duration_by_reason

# COMMAND ----------

# MAGIC %md
# MAGIC ##Number of fraud calls by location

# COMMAND ----------

from pyspark.sql.functions import *
@dlt.create_table(
  comment="fraud_calls_by_location",
  table_properties={
    "WeEnsure_delta.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraud_calls_by_location():
    stream_df = dlt.read("customer_stream_raw")
    fraud_calls_by_location = stream_df.groupBy("location") \
        .agg({"location": "count"}) \
        .withColumnRenamed("count(location)", "no_of_fraud_calls")
    return fraud_calls_by_location
