# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The Raw Billing_partition_1 data",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def billingp_raw():
    """
    Load and create the Raw Billing_partition_1 Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw Billing_partition_1 data.
    """
    billingp_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/basedata/unzipped/Billing_partition_1.csv", header=True, inferSchema=True)
    return billingp_df


# COMMAND ----------

@dlt.create_table(
  comment="The Raw Customers Info table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_info_raw():
    """
    Load and create the Raw Customers Info Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer info data.
    """
    customer_info_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/basedata/unzipped/Customer_information_updated.csv", header=True, inferSchema=True)
    return customer_info_df


# COMMAND ----------

@dlt.create_table(
  comment="The Raw Customers Rating table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_raw():
    """
    Load and create the Raw Customers Rating Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer rating data.
    """
    customer_rating_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/basedata/unzipped/Customer_rating.csv", header=True, inferSchema=True)
    return customer_rating_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw Plan table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    """
    Load and create the Raw Plans Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw Plans data.
    """
    plans_df = payment_df = spark.read.csv("dbfs:/mnt/basedata/unzipped/Plans_updated.csv", header=True, inferSchema=True)
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw device information",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def device_information_raw():
    """
    Load and create the Raw Device Information Delta Lake table.

    This function reads data from a JSON file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of device information.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw device information data.
    """
    # Read the JSON file into a DataFrame
    device_info_df = spark.read.json("dbfs:/mnt/basedata/unzipped/Device_Information.json")
    
    # Return the DataFrame
    return device_info_df

# COMMAND ----------


