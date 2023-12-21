# Databricks notebook source
#%run "./2DLTBronze"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

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
def convert_string_columns_to_date_format(df, columns_to_operate=[]):
    """
    Convert specified string columns to date format (YYYY-MM-DD).

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - columns_to_operate: list
      List of column names to operate on.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with specified columns converted to date format.
    """
    for column in columns_to_operate:
        # Check if the column exists in the DataFrame
        if column in df.columns:
            # Check if the column's data type is StringType
            if df.schema[column].dataType == 'string':
                # Convert string column to date format (YYYY-MM-DD)
                df = df.withColumn(column, col(column).cast(DateType()))
            else:
                print(f"Column '{column}' is not of type StringType. Skipping conversion.")
        else:
            print(f"Column '{column}' does not exist in the DataFrame. Skipping conversion.")

    return df
def convert_string_columns_to_numeric(df, columns_to_operate):
    """
    Convert specified string columns to numeric type or fill with zero.

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - columns_to_operate: list
      List of column names to operate on.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with specified columns converted to numeric type or filled with zero.
    """
    for column in columns_to_operate:
        # Check if the column exists in the DataFrame
        if column in df.columns:
            # Check if the column's data type is StringType
            if df.schema[column].dataType == 'string':
                # Try to convert string column to numeric type (DoubleType)
                df = df.withColumn(column, col(column).cast(DoubleType()))
                # Fill non-convertible values with zero
                df = df.withColumn(column, col(column).cast(DoubleType()).otherwise(0))
            else:
                print(f"Column '{column}' is not of type StringType. Skipping conversion.")
        else:
            print(f"Column '{column}' does not exist in the DataFrame. Skipping conversion.")

    return df
def convert_null_to_not_available(df, columns_to_operate):
    """
    Convert null values in specified columns to "Not Available".

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - columns_to_operate: list
      List of column names to operate on.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with null values in specified columns replaced with "Not Available".
    """
    for column in columns_to_operate:
        # Check if the column exists in the DataFrame
        if column in df.columns:
            # Replace null values with "Not Available"
            df = df.withColumn(column, when(col(column).isNull(), "Not Available").otherwise(col(column)))
        else:
            print(f"Column '{column}' does not exist in the DataFrame. Skipping conversion.")

    return df
def drop_rows_with_null(df, target_column):
    """
    Drop rows with null values in the specified column.

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - target_column: str
      The column on which to check for null values.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with rows containing null values in the specified column dropped.
    """
    # Check if the target column exists in the DataFrame
    if target_column in df.columns:
        # Drop rows with null values in the specified column
        df = df.na.drop(subset=[target_column])
    else:
        print(f"Column '{target_column}' does not exist in the DataFrame. Skipping row drop.")

    return df

def trim_data_in_columns(df, column_list=[]):
    """
    Trim whitespace from data in specified columns.

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - column_list: list
      List of column names to trim.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with whitespace trimmed from data in specified columns.
    """
    for column in column_list:
        # Check if the column exists in the DataFrame
        if column in df.columns:
            # Trim whitespace from data in the specified column
            df = df.withColumn(column, trim(col(column)))
        else:
            print(f"Column '{column}' does not exist in the DataFrame. Skipping trimming.")

    return df

import re

# def is_valid_email(email):
#     """
#     Check if the given email is a valid email address.

#     Parameters:
#     - email: str
#       The email address to validate.

#     Returns:
#     - str
#       The original email if valid, or "Not Available" if not valid.
#     """
#     # Use a simple regex pattern to check for a valid email format
#     email_pattern = re.compile(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$")
#     if email_pattern.match(email):
#         return email
#     else:
#         return "Not Available"

# # Create a UDF from the is_valid_email function
# udf_is_valid_email = udf(is_valid_email, StringType())

def is_valid_email(df, target_column):
    email_regex = r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"
    return df.withColumn(target_column,
                         when(regexp_extract(col(target_column), email_regex, 0) == col(target_column),
                              col(target_column)).otherwise("Not Available"))

##Example use case : df_validated = df.withColumn("Email", udf_is_valid_email("Email"))
def convert_numerical_to_string(df, column_list):
    """
    Convert numerical values to strings in specified columns.

    Parameters:
    - df: pyspark.sql.DataFrame
      The input DataFrame.
    - column_list: list
      List of column names to operate on.

    Returns:
    - pyspark.sql.DataFrame
      DataFrame with numerical values in specified columns converted to strings.
    """
    for column in column_list:
        # Check if the column exists in the DataFrame
        if column in df.columns:
            # Check if the column's data type is a numeric type
            if "int" in str(df.schema[column].dataType) or "double" in str(df.schema[column].dataType):
                # Convert numerical values to strings
                df = df.withColumn(column, col(column).cast(StringType()))
            else:
                print(f"Column '{column}' is not a numeric type. Skipping conversion.")
        else:
            print(f"Column '{column}' does not exist in the DataFrame. Skipping conversion.")

    return df
# def validate_and_mask_phone_number(phone_number):
#     """
#     Check if the phone number is of 10 digits. If not, replace it with "Not Available".

#     Parameters:
#     - phone_number: str
#       The phone number to validate.

#     Returns:
#     - str
#       The original phone number if valid, or "Not Available" if not valid.
#     """
#     # Remove non-digit characters
#     clean_phone_number = ''.join(c for c in phone_number if c.isdigit())

#     # Check if the cleaned phone number has exactly 10 digits
#     if len(clean_phone_number) == 10:
#         return phone_number
#     else:
#         return "Not Available"

# # Create a UDF from the validate_and_mask_phone_number function
# udf_validate_and_mask_phone_number = udf(validate_and_mask_phone_number, StringType())

# Apply the UDF to the PhoneNumber column
# df_validated = df.withColumn("PhoneNumber", udf_validate_and_mask_phone_number("PhoneNumber"))

def validate_and_mask_phone_number(df, target_column):
    return df.withColumn(target_column,
                         when(length(col(target_column)) > 10, "Not Available")
                         .otherwise(col(target_column)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning

# COMMAND ----------

@dlt.create_table(
  comment="The cleaning data of billing raws",
  table_properties={
    "WeEnsure.quality": "Silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid billing_id", "billing_id IS NOT NULL")
def billingp_silver():
    billingp_df = dlt.read('billingp_raw')
    billingp_df = convert_string_columns_to_date_format(billingp_df,["billing_date","due_date","payment_date"])
    billingp_df = convert_string_columns_to_numeric(billingp_df,["bill_amount"])
    billingp_df = convert_null_to_not_available(billingp_df,["customer_id"])
    billingp_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("billing_date").saveAsTable("billingp_silver")

    return billingp_df


# COMMAND ----------

@dlt.create_table(
  comment="The silver Customers Info table",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def customer_info_silver():
    customer_info_df = dlt.read('customer_info_raw')
    customer_info_df = convert_string_columns_to_date_format(customer_info_df,["dob"])
    customer_info_df = convert_numerical_to_string(customer_info_df,["customer_phone"])
    customer_info_df = convert_null_to_not_available(customer_info_df,["full_name","customer_email","customer_phone","system_status","connection_type","value_segment",])
    # customer_info_df = customer_info_df.withColumn("customer_email", udf_is_valid_email("customer_email"))
    # customer_info_df = customer_info_df.withColumn("customer_phone", udf_validate_and_mask_phone_number("customer_phone"))
    customer_info_df=validate_and_mask_phone_number(customer_info_df,"customer_phone")
    customer_info_df=is_valid_email(customer_info_df,"customer_email")
    customer_info_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("value_segment").saveAsTable("customer_info_silver")
    return customer_info_df


# COMMAND ----------

@dlt.create_table(
  comment="The silver Customers Rating table",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def customer_rating_silver():
    """
    Load and create the Raw Customers Rating Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer rating data.
    """
    customer_rating_df = dlt.read('customer_rating_raw')
    customer_rating_df = convert_null_to_not_available(customer_rating_df,["feedback"])
    customer_rating_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("rating").saveAsTable("customer_rating_silver")
    return customer_rating_df

# COMMAND ----------

@dlt.create_table(
  comment="The silver Plan table",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_silver():
    """
    Load and create the Raw Plans Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw Plans data.
    """
    plans_df = dlt.read('plans_raw')
    plans_df = drop_rows_with_null(plans_df,"tier")
    plans_df = convert_null_to_not_available(plans_df,["tier","Voice_Service","Mobile_Data","Message","Spam_Detection","Fraud_Prevention","OTT","Emergency"])
    plans_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("tier").saveAsTable("plans_silver")
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The silver device information",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def device_information_silver():
    """
    Load and create the Raw Device Information Delta Lake table.

    This function reads data from a JSON file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of device information.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw device information data.
    """
    # Read the JSON file into a DataFrame
    device_info_df = dlt.read('device_information_raw')
    device_info_df = convert_null_to_not_available(device_info_df,["brand_name","imei_tac","model_name","os_name","os_vendor"])
    device_info_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("brand_name").saveAsTable("device_information_silver")
    # Return the DataFrame
    return device_info_df

# COMMAND ----------


