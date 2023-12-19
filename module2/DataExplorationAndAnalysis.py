# Databricks notebook source
# MAGIC %run "./LoadBaseDataOnly"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions

# COMMAND ----------

import matplotlib.pyplot as plt
def plotCountOfNull(dataframe,datafameName="dataframes"):
    null_counts = [dataframe.where(dataframe[col].isNull()).count() for col in dataframe.columns]
    flag =True
    for value in null_counts:
        if value>0:
            flag = False
    if flag:
        print(f"{datafameName} have no null value to plot")
        return
            
    # Create a bar plot
    plt.figure(figsize=(12, 6))
    plt.bar(dataframe.columns, null_counts, color='skyblue')
    plt.xlabel('Columns')
    plt.ylabel('Number of Null Values')
    plt.title(f'Null Values Count for Each Column in {datafameName}')
    plt.xticks(rotation=45, ha='right')
    plt.show()

# COMMAND ----------

def showCountOfDuplicateForEachColumns(dataframe,dataframeName = "dataframe",columns=[]):
    print(f"Duplicates column count Report of {dataframeName}")
    for column in columns:
       countOfDuplicates = dataframe.groupBy(column).count().filter(col('count') > 1).count()
       print(f"Number of dublicate in {column} column is {countOfDuplicates}")


# COMMAND ----------

def getDistinct(dataframe,dataframeName="dataframe",columns=[]):
    print(f"Report of distinct columns values in {dataframeName}")
    for column in columns:
        distinct_values = dataframe.select(column).distinct()
        if True:
            print(f"distinct columns in {column}")
            distinct_values.show(truncate=False)

# COMMAND ----------

def getReportOnWhiteSpaceAroundStrigColumn(dataframe,dataframeName="dataframe",columns=[]):
    print(f"Report of white spaced values in {dataframeName}")
    for column in columns:
        whitespacecount = dataframe.filter(trim(col(column)) != col(column)).count()
        print(f"{column} has {whitespacecount} white spaced column")

# COMMAND ----------

# MAGIC %md
# MAGIC ###billing_partition_df exploration and analysis

# COMMAND ----------

billing_partition_df.show()

# COMMAND ----------

billing_partition_df.printSchema()

# COMMAND ----------

#Bill amount should be of numeric type
billing_partition_df = billing_partition_df.withColumn("bill_amount", col("bill_amount").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####checking for null values billing partition table

# COMMAND ----------

plotCountOfNull(billing_partition_df,"Billing partition table")

# COMMAND ----------

# MAGIC %md
# MAGIC ####checking for duplicate row in billing partition table

# COMMAND ----------

showCountOfDuplicateForEachColumns(billing_partition_df,"billing partition table",["billing_id","Customer_Id"])

# COMMAND ----------

#duplicates in Customer_Id column can be possibility
#In conclusion billing partition table have no duplicates to  drop
#As infering schema automatically set ant date column as date type and on analysing the column on date type show no null values
#data in billing partition is already clean

# COMMAND ----------

# MAGIC %md
# MAGIC ###customer_information_df exploration and analysis

# COMMAND ----------

customer_information_df.show()

# COMMAND ----------

customer_information_df.printSchema()

# COMMAND ----------

plotCountOfNull(customer_information_df,"customer information table")

# COMMAND ----------

getReportOnWhiteSpaceAroundStrigColumn(customer_information_df,"customer information table",["Full_Name","Customer_Email"])

# COMMAND ----------

showCountOfDuplicateForEachColumns(customer_information_df,"customer information table",["Customer_id","Customer_Email"])

# COMMAND ----------

customer_information_df.groupBy("Customer_id").count().filter(col("count")>1).join(customer_information_df,on="Customer_id").drop("count").orderBy("Customer_id").show(truncate=False)

# COMMAND ----------

customer_information_df.groupBy("Customer_Email").count().filter(col("count")>1).join(customer_information_df,on="Customer_Email").drop("count").orderBy("Customer_Email").show(truncate=False)

# COMMAND ----------

getDistinct(customer_information_df,"customer information table",["system_status","Connection_type","value_segment"])

# COMMAND ----------

#Report on phone no length
phone_length_number_report = customer_information_df.select("Customer_Phone").withColumn("Customer_Phone", col("Customer_Phone").cast("string")).groupBy(length("Customer_Phone").alias("phone_length")).count().dropna().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###customer_rating_df exploration and analysis

# COMMAND ----------

customer_rating_df.show()

# COMMAND ----------

customer_rating_df.printSchema()

# COMMAND ----------

plotCountOfNull(customer_rating_df,"customer rating table")

# COMMAND ----------

getReportOnWhiteSpaceAroundStrigColumn(customer_rating_df,"customer rating table",["feedback"])

# COMMAND ----------

getDistinct(customer_rating_df,"customer rating table",["feedback"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###device_information_df exploration and analysis

# COMMAND ----------

device_information_df.show()

# COMMAND ----------

device_information_df.printSchema()

# COMMAND ----------

plotCountOfNull(device_information_df,"device information table")

# COMMAND ----------

getDistinct(device_information_df,"device information table",["brand_name","model_name","os_name","os_vendor"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###plan_df exploration and analysis

# COMMAND ----------

plan_df.show()

# COMMAND ----------

plan_df.printSchema()

# COMMAND ----------

plotCountOfNull(plan_df,"plan table")
