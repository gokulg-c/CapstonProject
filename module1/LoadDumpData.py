# Databricks notebook source
dbutils.fs.unmount("/mnt/wetelcodata/")

# COMMAND ----------

container_name = "data"
account_name = "wetelco"
storage_account_key = "mG0nyBLqrK4T4SJnHOnc3ZBAf/Nkeu7f57Jro3o9ZJAz0ipXPtHkDlOCvaPefzadKSCl5UD97XiL+AStlZdB0Q=="

dbutils.fs.mount(
source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
mount_point = "/mnt/wetelcodata/",
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
  )

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# %sql
# create database main.wetelco_bronzelayerdb;

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

# COMMAND ----------

# %sql
# drop database main.wetelco_goldlayerdb;

# COMMAND ----------



# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/d92d5094-56ee-43b1-afc1-d6d844d547d5_83d04ac6-cb74-4a96-a06a-e0d5442aa126_Telecom.zip"

local_zip_file_path = "/dbfs/mnt/wetelcodata/data/data.zip"
local_unzip_dir = "/dbfs/mnt/wetelcodata/data/unzipped_data"

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=capstonp;AccountKey=+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A==;EndpointSuffix=core.windows.net"

folder_name = "unzipped"  

# COMMAND ----------

subprocess.run(["wget", "-O", local_zip_file_path, zip_file_url], check=True)
subprocess.run(["unzip", "-q", local_zip_file_path, "-d", local_unzip_dir], check=True)

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_files = os.listdir(local_unzip_dir)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    blob_path = os.path.join(folder_name, file_name)

    blob_client = container_client.get_blob_client(blob_path)

    with open(local_file_path, "rb") as data:

        blob_client.upload_blob(data)

 

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

 
os.remove(local_zip_file_path)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    os.remove(local_file_path)

print("Cleaned up local files")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/wetelcodata/data"))

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/wetelcodata/unzipped"))

# COMMAND ----------

path = "dbfs:/mnt/wetelcodata/data/unzipped_data/Billing_partition_1.csv"
billing_partition_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

billing_partition_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/wetelcodata/data/unzipped_data/Customer_information.csv"
customer_information_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

customer_information_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/wetelcodata/data/unzipped_data/Customer_rating.csv"
customer_rating_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

customer_rating_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/wetelcodata/data/unzipped_data/Plans.csv"
plan_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

plan_df.printSchema()

# COMMAND ----------

device_information_df = spark.read.json("dbfs:/mnt/wetelcodata/data/unzipped_data/Device_Information.json")

# COMMAND ----------

device_information_df.printSchema()

# COMMAND ----------

plan_df.show()
customer_rating_df.show()
customer_information_df.show()
device_information_df.show()
billing_partition_df.show()

# COMMAND ----------

def replace_spaces_with_underscore(df): ##code can be used in harmonizations
    """
    Replace spaces in column names with underscores for a DataFrame.
 
    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
 
    Returns:
    - pyspark.sql.DataFrame
      DataFrame with column names having spaces replaced by underscores.
    """
    # Extract the current column names
    current_columns = df.columns
   
    # Create a mapping of current column names to modified names
    column_mapping = {col: col.replace(" ", "_") for col in current_columns}
   
    # Use a loop to rename the columns
    for col in current_columns:
        df = df.withColumnRenamed(col, column_mapping[col])
   
    return df

# COMMAND ----------

def convert_columns_to_lowercase(df): ##code can be used in harmonizations
    """
    Convert all column names in a DataFrame to lowercase.
 
    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
 
    Returns:
    - pyspark.sql.DataFrame
      DataFrame with lowercase column names.
    """
    # Extract the current column names
    current_columns = df.columns
   
    # Create a mapping of current column names to lowercase names
    column_mapping = {col: col.lower() for col in current_columns}
   
    # Use a loop to rename the columns
    for col in current_columns:
        df = df.withColumnRenamed(col, column_mapping[col])
   
    return df

# COMMAND ----------

plan_df=replace_spaces_with_underscore(plan_df)
customer_rating_df=replace_spaces_with_underscore(customer_rating_df)
customer_information_df=replace_spaces_with_underscore(customer_information_df)
device_information_df=replace_spaces_with_underscore(device_information_df)
billing_partition_df=replace_spaces_with_underscore(billing_partition_df)

# COMMAND ----------

plan_df = convert_columns_to_lowercase(plan_df)
customer_rating_df= convert_columns_to_lowercase(customer_rating_df)
customer_information_df= convert_columns_to_lowercase(customer_information_df)
device_information_df= convert_columns_to_lowercase(device_information_df)
billing_partition_df= convert_columns_to_lowercase(billing_partition_df)

# COMMAND ----------

plan_df.show()
customer_rating_df.show()
customer_information_df.show()
device_information_df.show()
billing_partition_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database main.wetelco_bronzelayerdb if not exists;

# COMMAND ----------

from pyspark.sql import DataFrame

def save_dataframes_in_delta(dataframes: dict, paths: dict,databasename : str):
    """
    Saves Spark DataFrames to delta format and organizes them into different folders.

    Args:
    - dataframes (dict): Dictionary of DataFrame names and their corresponding DataFrames.
    - paths (dict): Dictionary of paths for each DataFrame to be saved.
    -databasename : database where table going to save

    Example:
    dataframes = {
        "plan_df": plan_df,
        "customer_rating_df": customer_rating_df,
        "customer_information_df": customer_information_df,
        "device_information_df": device_information_df,
        "billing_partition_df": billing_partition_df
    }
    paths = {
        "plan_df": "/mnt/basedata/BronzeLayerData/Plans/",
        "customer_rating_df": "/mnt/basedata/BronzeLayerData/Customer_Rating/",
        "customer_information_df": "/mnt/basedata/BronzeLayerData/Customer_Information/",
        "device_information_df": "/mnt/basedata/BronzeLayerData/Device_Information/",
        "billing_partition_df": "/mnt/basedata/BronzeLayerData/Billing_Information/"
    }

    """
    for df_name, df in dataframes.items():
        if isinstance(df, DataFrame) and df.count() > 0:
            path = paths.get(df_name)
            if path:
                df.write.format("delta").mode("overwrite").option("path", path).saveAsTable(f"{databasename}.{df_name}")
            else:
                print(f"Path not found for DataFrame '{df_name}'. Skipping...")
        else:
            print(f"Invalid DataFrame or empty DataFrame '{df_name}'. Skipping...")
# billing_partition_df.write.format("delta").mode("overwrite").option("/mnt/wetelcodata/demo", path).saveAsTable("main.wetelco_bronzelayerdb.billing_partition_df")




# COMMAND ----------

# MAGIC %sql
# MAGIC create table main.wetelco_bronzelayerdb.billing_partition_df (
# MAGIC billing_id string,
# MAGIC customer_id string,
# MAGIC billing_date date,
# MAGIC due_date date,
# MAGIC payment_date date,
# MAGIC bill_amount string
# MAGIC )

# COMMAND ----------

billing_partition_df.write.format("delta").mode("overwrite").option("path","dbfs:/mnt/wetelcodata/BronzeLayerData").saveAsTable("main.wetelco_bronzelayerdb.billing_partition_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database  main.wetelco_bronzelayerdb casade;

# COMMAND ----------

dataframes = {
    "plan_df": plan_df,
    "customer_rating_df": customer_rating_df,
    "customer_information_df": customer_information_df,
    "device_information_df": device_information_df,
    "billing_partition_df": billing_partition_df
}

paths = {
    "plan_df": "/mnt/wetelcodata/BronzeLayerData/Plans/",
    "customer_rating_df": "/mnt/wetelcodata/BronzeLayerData/Customer_Rating/",
    "customer_information_df": "/mnt/wetelcodata/BronzeLayerData/Customer_Information/",
    "device_information_df": "/mnt/wetelcodata/BronzeLayerData/Device_Information/",
    "billing_partition_df": "/mnt/wetelcodata/BronzeLayerData/Billing_Information/"
}

save_dataframes_in_delta(dataframes, paths,"main.wetelco_bronzelayerdb")

# COMMAND ----------


