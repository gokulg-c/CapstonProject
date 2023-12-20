# Databricks notebook source
# MAGIC %md
# MAGIC ##Data access

# COMMAND ----------

#code to access data from unity catalog database
#code here...
#dataframes : Billing_partition_df, Customer_information_df, Customer_rating_df, Device_Information_df, Plans_df

# COMMAND ----------

#temporary code to access the data
display(dbutils.fs.ls("/mnt/basedata/BronzeLayerData/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/basedata/BronzeLayerData/Billing_Information/"))

# COMMAND ----------

Billing_partition_df = spark.read.parquet("/mnt/basedata/BronzeLayerData/Billing_Information/")
Customer_information_df = spark.read.parquet("/mnt/basedata/BronzeLayerData/Customer_Information/")
Customer_rating_df = spark.read.parquet("/mnt/basedata/BronzeLayerData/Customer_Rating/")
Device_Information_df = spark.read.parquet("/mnt/basedata/BronzeLayerData/Device_Information/")
Plans_df = spark.read.parquet("/mnt/basedata/BronzeLayerData/Plans/")

# COMMAND ----------

# MAGIC %run ./DataCleaningFunctions

# COMMAND ----------

#droping rows with id as null values

# COMMAND ----------

Billing_partition_df = drop_rows_with_null(Billing_partition_df,"billing_id")
Customer_information_df = drop_rows_with_null(Customer_information_df,"customer_id")
Customer_rating_df = drop_rows_with_null(Customer_rating_df,"customer_id")
Device_Information_df = drop_rows_with_null(Device_Information_df,"customer_id")
Plans_df = drop_rows_with_null(Plans_df,"tier")

# COMMAND ----------

#Taking care of date type for particular columns

# COMMAND ----------

Billing_partition_df = convert_string_columns_to_date_format(Billing_partition_df,["billing_date","due_date"])
Customer_information_df = convert_string_columns_to_date_format(Customer_information_df,["dob"])

# COMMAND ----------

#Taking care of numerical data

# COMMAND ----------

Billing_partition_df = convert_string_columns_to_numeric(Billing_partition_df,["bill_amount"])

# COMMAND ----------

#Taking care of String numberical data like phone number as if not available we have mention "Not Available" instead of zero or null

# COMMAND ----------

Customer_information_df =  convert_numerical_to_string(Customer_information_df,["customer_phone"])

# COMMAND ----------

#Taking care of null value in string data columns

# COMMAND ----------

Customer_information_df = convert_null_to_not_available(Customer_information_df,["full_name","customer_email","customer_phone","system_status","connection_type","value_segment",])
Billing_partition_df = convert_null_to_not_available(Billing_partition_df,["customer_id"])
Customer_rating_df = convert_null_to_not_available(Customer_rating_df,["feedback"])
Device_Information_df = convert_null_to_not_available(Device_Information_df,["brand_name","imei_tac","model_name","os_name","os_vendor"])
Plans_df = convert_null_to_not_available(Plans_df,["tier","Voice_Service","Mobile_Data","Message","Spam_Detection","Fraud_Prevention","OTT","Emergency"])

# COMMAND ----------

#Check for valid email if not valid replace with "Not Available"

# COMMAND ----------

Customer_information_df = Customer_information_df.withColumn("customer_email", udf_is_valid_email("customer_email"))

# COMMAND ----------

#Check for correct phone number

# COMMAND ----------

Customer_information_df = Customer_information_df.withColumn("customer_phone", udf_validate_and_mask_phone_number("customer_phone"))

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
        "plan_df": "/mnt/basedata/somelayer/Plans/",
        "customer_rating_df": "/mnt/basedata/somelayer/Customer_Rating/",
        "customer_information_df": "/mnt/basedata/somelayer/Customer_Information/",
        "device_information_df": "/mnt/basedata/somelayer/Device_Information/",
        "billing_partition_df": "/mnt/basedata/somelayer/Billing_Information/"
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




# COMMAND ----------

dataframes = {
    "plan_df": Plans_df,
    "customer_rating_df": Customer_rating_df,
    "customer_information_df": Customer_information_df,
    "device_information_df": Device_Information_df,
    "billing_partition_df": Billing_partition_df
}

paths = {
    "plan_df": "/mnt/basedata/SilverLayerData/Plans/",
    "customer_rating_df": "/mnt/basedata/SilverLayerData/Customer_Rating/",
    "customer_information_df": "/mnt/basedata/SilverLayerData/Customer_Information/",
    "device_information_df": "/mnt/basedata/SilverLayerData/Device_Information/",
    "billing_partition_df": "/mnt/basedata/SilverLayerData/Billing_Information/"
}
 
def save_dataframes_in_delta(dataframes: dict, paths: dict,databasename : str):
(dataframes, paths,"silverlayerdata")

# COMMAND ----------


