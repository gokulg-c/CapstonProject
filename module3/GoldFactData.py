# Databricks notebook source
#%run "./3DLTSilver"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Monthly Billing Totals

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

# Assuming 'spark' is your SparkSession
# You may need to adjust the configurations and paths according to your environment

def create_monthly_billing_table(billing_info_df):
    # Transform the DataFrame according to the SQL query
    monthly_billing_totals_df = billing_info_df \
        .withColumn("billing_year", year("billing_date")) \
        .withColumn("billing_month", month("billing_date")) \
        .groupBy("billing_year", "billing_month") \
        .agg({"bill_amount": "sum"}) \
        .withColumnRenamed("sum(bill_amount)", "total_bill_amount") \
        .orderBy("billing_year", "billing_month")

    # # Create the Delta table
    # monthly_billing_totals_df.write.format("delta") \
    #     .mode("overwrite") \
    #     .saveAsTable("goldenlayer.monthly_billing_totals")

    return monthly_billing_totals_df

# COMMAND ----------


@dlt.create_table(
    comment="Aggregated Billing",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def monthly_billing_totals():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    result_df = create_monthly_billing_table(billingp_df)
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Customer Lifetime Value (CLV)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, datediff, max as spark_max, min as spark_min

def generate_customer_lifetime_value(customer_info_df, billing_info_df):
    customer_lifetime_value_df = customer_info_df.join(
        billing_info_df,
        on="customer_id"
    ).groupBy(
        "customer_id",
        "full_name",
        "value_segment"
    ).agg(
        sum("bill_amount").alias("total_billing_amount"),
        count("billing_id").alias("total_transactions"),
        datediff(max("payment_date"), min("billing_date")).alias("days_active"),
        (sum("bill_amount") / count("billing_id")).alias("avg_transaction_amount"),
        (sum("bill_amount") / datediff(max("payment_date"), min("billing_date"))).alias("daily_revenue")
    )

    return customer_lifetime_value_df

# COMMAND ----------

@dlt.create_table(
    comment="customer_lifetime_value",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_lifetime_value():
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    customer_lifetime_value_df = generate_customer_lifetime_value(customer_info_df, billingp_df)
    return customer_lifetime_value_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Churn Prediction Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, when

def churn_prediction_data(ci, cr, bi, di):
    churn_data = ci \
        .join(cr, on="customer_id", how="left") \
        .join(bi, on="customer_id", how="left") \
        .join(di, on="customer_id", how="left") \
        .groupBy("customer_id", "value_segment", "system_status") \
        .agg(
            avg(col("rating")).alias("avg_rating"),
            count(col("billing_id")).alias("total_transactions"),
            count(col("imei_tac")).alias("total_devices_used"),
            count(col("payment_date")).alias("days_active"),
            sum(when(col("system_status") == "Suspended", 1).otherwise(0)).alias("suspended_status_count")
        ) \
        .withColumnRenamed("value_segment", "tier") \
        .withColumn("avg_rating", col("avg_rating").cast("int"))

    return churn_data

# COMMAND ----------

@dlt.create_table(
    comment="churn_prediction_data",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def churn_prediction():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    churn_df = churn_prediction_data(customer_info_df, customer_rating_df,billingp_df, device_info_df)
    return churn_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Customer Satisfaction Index (CSI) Analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, when, datediff


def customer_satisfaction_index(ci, cr, bi, pi, di):
    satisfaction_data = ci \
        .join(cr, on="customer_id", how="left") \
        .join(bi, on="customer_id", how="left") \
        .join(pi, ci.value_segment == pi.tier, how="left") \
        .join(di, on="customer_id", how="left") \
        .groupBy("customer_id", "full_name", "value_segment") \
        .agg(
            avg(col("rating")).alias("avg_rating"),
            sum(when(col("voice_service") == 'Yes', 1).otherwise(0)).alias("voice_service_count"),
            sum(when(col("mobile_data") == 'Yes', 1).otherwise(0)).alias("mobile_data_count"),
            count(col("billing_id")).alias("total_transactions"),
            sum(col("bill_amount")).alias("total_billing_amount"),
            datediff(max(col("payment_date")), min(col("billing_date"))).alias("customer_tenure"),
            count(col("imei_tac")).alias("total_devices_used"),
            count(col("payment_date")).alias("days_active"),
            sum(when(col("system_status") == 'Suspended', 1).otherwise(0)).alias("suspended_status_count"),
            avg(when(col("rating") <= 2, 1).otherwise(0)).alias("low_rating_ratio"),
            avg(when(col("rating") >= 4, 1).otherwise(0)).alias("high_rating_ratio")
        )

    return satisfaction_data



# COMMAND ----------

@dlt.create_table(
    comment="customer_satisfaction_index",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_satisfaction():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    satisfaction_df = customer_satisfaction_index(customer_info_df, customer_rating_df,billingp_df, plans_df, device_info_df)
    return satisfaction_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Brand Preference Analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

def customer_brand_preference(di, ci, bi, cr):
    brand_preference_data = di \
        .join(ci, on="customer_id", how="left") \
        .join(bi, on="customer_id", how="left") \
        .join(cr, on="customer_id", how="left") \
        .groupBy("brand_name") \
        .agg(
            countDistinct(col("customer_id")).alias("customer_count"),
            countDistinct(col("imei_tac")).alias("device_count"),
            countDistinct(col("billing_id")).alias("total_transactions"),
            countDistinct(col("payment_date")).alias("days_active")
        )

    return brand_preference_data


# COMMAND ----------

@dlt.create_table(
    comment="customer_brand_preference",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_brand_pref():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    preference_df = customer_brand_preference(device_info_df,customer_info_df,billingp_df,customer_rating_df)
    return preference_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Customer Aggregate

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Assuming you have a SparkSession named 'spark'
def customer_fact_aggregate(ci, bi, di):
    fact_aggregate_data = ci \
        .join(bi,  on="customer_id", how="inner") \
        .join(di,  on="customer_id", how="inner") \
        .groupBy("customer_id", "full_name", "value_segment", "os_name", "brand_name") \
        .agg(
            sum(col("bill_amount")).alias("total_amount_paid")
        ) \
        .withColumnRenamed("customer_id", "id") \
        .withColumnRenamed("full_name", "name") \
        .withColumnRenamed("value_segment", "tier") \
        .withColumnRenamed("os_name", "os") \
        .withColumnRenamed("brand_name", "brand_used")

    return fact_aggregate_data

# COMMAND ----------

@dlt.create_table(
    comment="customer_brand_preference",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_fact_aggre():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    fact_aggregate_df = customer_fact_aggregate(customer_info_df,billingp_df, device_info_df)
    return fact_aggregate_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Tier by ratings with total amount

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum


def tier_by_ratings_with_total_amount(ci, cr, bi):


    tier_ratings_amount = ci \
        .join(cr, on="customer_id", how="inner") \
        .join(bi, on="customer_id", how="inner") \
        .groupBy("value_segment") \
        .agg(
            avg(col("rating")).alias("AVG_Rating"),
            sum(col("bill_amount")).alias("total_amount")
        ) \
        .withColumnRenamed("value_segment", "tier")

    return tier_ratings_amount



# COMMAND ----------

@dlt.create_table(
    comment="tier_by_ratings_with_total_amount",
    table_properties={
        "wetelco.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def tier_by_ratings():
    
    # Read Delta tables
    billingp_df = dlt.read('billingp_silver')
    customer_info_df = dlt.read('customer_info_silver')
    customer_rating_df = dlt.read('customer_rating_silver')
    plans_df = dlt.read('plans_silver')
    device_info_df = dlt.read('device_information_silver')
    tier_ratings_amount_df = tier_by_ratings_with_total_amount(customer_info_df,customer_rating_df, billingp_df)
    return tier_ratings_amount_df
