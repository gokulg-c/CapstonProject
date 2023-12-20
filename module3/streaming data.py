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

display(dbutils.fs.ls("/mnt/wetelcostreams"))

# COMMAND ----------

df=spark.read.parquet("/mnt/wetelcostreams")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

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

display(dbutils.fs.mounts())

# COMMAND ----------

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


# COMMAND ----------

stream_df.display()

# COMMAND ----------

paths = {
    "plan_df": "/mnt/basedata/SilverLayerData/Plans/",
    "customer_rating_df": "/mnt/basedata/SilverLayerData/Customer_Rating/",
    "customer_information_df": "/mnt/basedata/SilverLayerData/Customer_Information/",
    "device_information_df": "/mnt/basedata/SilverLayerData/Device_Information/",
    "billing_partition_df": "/mnt/basedata/SilverLayerData/Billing_Information/"
}
# Assuming you have loaded DataFrames for each path, for example:
plan_df = spark.read.parquet(paths["plan_df"])
customer_rating_df = spark.read.parquet(paths["customer_rating_df"])
customer_information_df = spark.read.parquet(paths["customer_information_df"])
device_information_df = spark.read.parquet(paths["device_information_df"])
billing_partition_df = spark.read.parquet(paths["billing_partition_df"])

# COMMAND ----------

display(customer_information_df)

# COMMAND ----------

stream_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##identify if any of the fraud callers are existing customers or any of the existing customers receiving calls from fraud numbers

# COMMAND ----------

joined_stream_df = stream_df.join(
    customer_information_df,
    (stream_df.caller_number == customer_information_df.customer_phone) |
    (stream_df.receiver_number == customer_information_df.customer_phone),
    "inner"
)
display(joined_stream_df)

# COMMAND ----------

query = joined_stream_df.writeStream.outputMode("append").format("console").start()


# COMMAND ----------

# MAGIC %md
# MAGIC ##duration of call by suspected reason

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_utc_timestamp


# Convert start_time and end_time to timestamp
stream_df = stream_df.withColumn("start_time", from_utc_timestamp(unix_timestamp("start_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp"), "UTC"))
stream_df = stream_df.withColumn("end_time", from_utc_timestamp(unix_timestamp("end_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp"), "UTC"))

# Calculate duration in seconds
stream_df = stream_df.withColumn("call_duration", (unix_timestamp("end_time") - unix_timestamp("start_time")))

# Group by suspected_reason and calculate the average duration
average_duration_by_reason = stream_df.groupBy("suspected_reason").agg({"call_duration": "avg"})




# COMMAND ----------

display(average_duration_by_reason)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Number of fraud calls by location

# COMMAND ----------

fraud_calls_by_location = stream_df.groupBy("location") \
    .agg({"location": "count"}) \
    .withColumnRenamed("count(location)", "no_of_fraud_calls")
display(fraud_calls_by_location)

# COMMAND ----------

fraud_calls_by_location.columns
