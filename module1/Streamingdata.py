# Databricks notebook source
# MAGIC %md
# MAGIC ##Streaming Data

# COMMAND ----------

import dlt

# COMMAND ----------

# container_name = "basedata"
# account_name = "capstonp"
# storage_account_key = "+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A=="
# # mount_point = "/mnt/basedata"
# # # Check if the storage is already mounted
# # mounts = dbutils.fs.mounts()
# # is_mounted = False
# # for mount in mounts:
# #   if mount.mountPoint == mount_point:
# #     is_mounted = True
 
# # # If not mounted, then mount it
# # if not is_mounted:
# #     dbutils.fs.mount(
# #       source='wasbs://{0}@{1}.blob.core.windows.net'.format(container_name,account_name),
# #       mount_point = mount_point,
# #       extra_configs  = {f"fs.azure.account.key.{account_name}.blob.core.windows.net" : storage_account_key}
# #     )
# #     print(f"Storage mounted at {mount_point}")
# # else:
# #     print(f"Storage is already mounted at {mount_point}")

# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/basedata",
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
#   )

# COMMAND ----------

# container_name = "wetelcostreams"
# account_name = "adlsstoragedata01"
# storage_account_key = "tBwtMqWlyr9ToC74Jxtq1UrA9aFi8fugoJo2SaKHxbwQnSIimMs6QjLW/Xw2Ujpk6M/wb9F9BXeB+AStk6vtGQ=="

# # mount_point="/mnt/wetelcostreams"
# # # Check if the storage is already mounted
# # mounts = dbutils.fs.mounts()
# # is_mounted = False
# # for mount in mounts:
# #   if mount.mountPoint == mount_point:
# #     is_mounted = True
 
# # # If not mounted, then mount it
# # if not is_mounted:
# #     dbutils.fs.mount(
# #       source='wasbs://{0}@{1}.blob.core.windows.net'.format(container_name,account_name),
# #       mount_point = mount_point,
# #       extra_configs  = {f"fs.azure.account.key.{account_name}.blob.core.windows.net" : storage_account_key}
# #     )
# #     print(f"Storage mounted at {mount_point}")
# # else:
# #     print(f"Storage is already mounted at {mount_point}")
# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/wetelcostreams",
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
#   )

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers stream data.",
  table_properties={
    "wetelco.quality": "bronze",
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
        .option("cloudFiles.schemaLocation", "/mnt/basedata/outputschema") \
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
