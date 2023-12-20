-- Databricks notebook source
-- MAGIC %sql
-- MAGIC USE CATALOG wetelco;

-- COMMAND ----------

-- Create the database
CREATE DATABASE IF NOT EXISTS capstone;

-- Use the database
USE capstone;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt/basedata/SilverLayerData"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet("/mnt/basedata/SilverLayerData/Billing_Information/").createOrReplaceTempView("Billing_Information_df")
-- MAGIC spark.read.parquet("/mnt/basedata/SilverLayerData/Customer_Information/").createOrReplaceTempView("Customer_Information_df")
-- MAGIC spark.read.parquet("/mnt/basedata/SilverLayerData/Customer_Rating/").createOrReplaceTempView("Customer_Rating_df")
-- MAGIC spark.read.parquet("/mnt/basedata/SilverLayerData/Device_Information/").createOrReplaceTempView("Device_Information_df")
-- MAGIC spark.read.parquet("/mnt/basedata/SilverLayerData/Plans/").createOrReplaceTempView("Plans_df")

-- COMMAND ----------

-- Billing Information
CREATE TABLE IF NOT EXISTS capstone.Billing_Information
USING DELTA
AS
SELECT *
FROM Billing_Information_df;

-- Customer Information
CREATE TABLE IF NOT EXISTS capstone.Customer_Information
USING DELTA
AS
SELECT *
FROM Customer_Information_df;

-- Customer Rating
CREATE TABLE IF NOT EXISTS capstone.Customer_Rating
USING DELTA
AS
SELECT *
FROM Customer_Rating_df;

-- Device Information
CREATE TABLE IF NOT EXISTS capstone.Device_Information
USING DELTA
AS
SELECT *
FROM Device_Information_df;

-- Plans
CREATE TABLE IF NOT EXISTS capstone.Plans
USING DELTA
AS
SELECT *
FROM Plans_df;

-- COMMAND ----------

--Make Views of cleaned data

-- COMMAND ----------

-- Create the database
CREATE DATABASE IF NOT EXISTS goldenlayer;

-- Use the database
USE goldenlayer;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Aggregated Facts for Downstream Consumption
-- MAGIC

-- COMMAND ----------

-- Aggregating facts based on customer_id and summing bill_amount
CREATE OR REPLACE TEMPORARY VIEW goldenlayer.aggregated_facts AS
SELECT
    customer_id,
    COUNT(*) AS total_records,
    SUM(bill_amount) AS total_bill_amount
FROM fact_table
GROUP BY customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Monthly Billing Totals

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.monthly_billing_totals
USING DELTA
AS
SELECT
    YEAR(billing_date) AS billing_year,
    MONTH(billing_date) AS billing_month,
    SUM(bill_amount) AS total_bill_amount
FROM capstone.billing_information
GROUP BY YEAR(billing_date), MONTH(billing_date);

-- COMMAND ----------

SELECT * FROM goldenlayer.monthly_billing_totals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Average Rating by Connection Type

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW avg_rating_by_connection_type AS
SELECT
    connection_type,
    AVG(rating) AS avg_rating
FROM capstone.customer_rating
GROUP BY connection_type;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldlayer.avg_rating_by_connection_type
USING DELTA
AS
SELECT *
FROM avg_rating_by_connection_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Total Bill Amount by Tier

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.total_bill_amount_by_tier
USING DELTA
AS
SELECT
    ci.value_segment AS tier,
    SUM(bi.bill_amount) AS total_bill_amount
FROM capstone.customer_information ci
INNER JOIN capstone.billing_information bi ON ci.customer_id = bi.customer_id
GROUP BY ci.value_segment;

-- COMMAND ----------

SELECT * FROM goldenlayer.total_bill_amount_by_tier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Customer Lifetime Value (CLV)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.customer_lifetime_value
USING DELTA
AS
SELECT
    ci.customer_id,
    ci.full_name,
    ci.value_segment AS tier,
    SUM(bi.bill_amount) AS total_billing_amount,
    COUNT(DISTINCT bi.billing_id) AS total_transactions,
    DATEDIFF(MAX(bi.payment_date), MIN(bi.billing_date)) AS days_active,
    SUM(bi.bill_amount) / COUNT(DISTINCT bi.billing_id) AS avg_transaction_amount,
    SUM(bi.bill_amount) / DATEDIFF(MAX(bi.payment_date), MIN(bi.billing_date)) AS daily_revenue
FROM capstone.customer_information ci
JOIN capstone.billing_information bi ON ci.customer_id = bi.customer_id
GROUP BY ci.customer_id, ci.full_name, ci.value_segment;

-- COMMAND ----------

SELECT * FROM goldenlayer.customer_lifetime_value

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Churn Prediction Data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.churn_prediction_data
USING DELTA
AS
SELECT
    ci.customer_id,
    ci.value_segment AS tier,
    AVG(cr.rating) AS avg_rating,
    COUNT(DISTINCT bi.billing_id) AS total_transactions,
    COUNT(DISTINCT di.imei_tac) AS total_devices_used,
    COUNT(DISTINCT bi.payment_date) AS days_active,
    SUM(CASE WHEN ci.system_status = 'Suspended' THEN 1 ELSE 0 END) AS suspended_status_count
FROM capstone.customer_information ci
LEFT JOIN capstone.customer_rating cr ON ci.customer_id = cr.customer_id
LEFT JOIN capstone.billing_information bi ON ci.customer_id = bi.customer_id
LEFT JOIN capstone.device_information di ON ci.customer_id = di.customer_id
GROUP BY ci.customer_id, ci.value_segment, ci.system_status;

-- COMMAND ----------

SELECT * FROM goldenlayer.churn_prediction_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Network Usage Analysis(NO NEED)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.network_usage_analysis
USING DELTA
AS
SELECT
    ci.customer_id,
    ci.value_segment AS tier,
    SUM(CASE WHEN pi.voice_service = 'Yes' THEN 1 ELSE 0 END) AS voice_service_count,
    SUM(CASE WHEN pi.mobile_data = 'Yes' THEN 1 ELSE 0 END) AS mobile_data_count,
    SUM(CASE WHEN pi.ott = 'Yes' THEN 1 ELSE 0 END) AS ott_service_count
FROM capstone.customer_information ci
LEFT JOIN capstone.plans pi ON ci.value_segment = pi.tier
GROUP BY ci.customer_id, ci.value_segment;

-- COMMAND ----------

SELECT * FROM goldenlayer.network_usage_analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Customer Satisfaction Index (CSI) Analysis

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.customer_satisfaction_index
USING DELTA
AS
SELECT
    ci.customer_id,
    ci.full_name,
    AVG(cr.rating) AS avg_rating,
    ci.value_segment AS tier,
    SUM(CASE WHEN pi.voice_service = 'Yes' THEN 1 ELSE 0 END) AS voice_service_count,
    SUM(CASE WHEN pi.mobile_data = 'Yes' THEN 1 ELSE 0 END) AS mobile_data_count,
    COUNT(DISTINCT bi.billing_id) AS total_transactions,
    SUM(bi.bill_amount) AS total_billing_amount,
    DATEDIFF(MAX(bi.payment_date), MIN(bi.billing_date)) AS customer_tenure,
    COUNT(DISTINCT di.imei_tac) AS total_devices_used,
    COUNT(DISTINCT bi.payment_date) AS days_active,
    SUM(CASE WHEN ci.system_status = 'Suspended' THEN 1 ELSE 0 END) AS suspended_status_count,
    AVG(CASE WHEN cr.rating <= 2 THEN 1 ELSE 0 END) AS low_rating_ratio,
    AVG(CASE WHEN cr.rating >= 4 THEN 1 ELSE 0 END) AS high_rating_ratio
FROM capstone.customer_information ci
LEFT JOIN capstone.customer_rating cr ON ci.customer_id = cr.customer_id
LEFT JOIN capstone.billing_information bi ON ci.customer_id = bi.customer_id
LEFT JOIN capstone.plans pi ON ci.value_segment = pi.tier
LEFT JOIN capstone.device_information di ON ci.customer_id = di.customer_id
GROUP BY ci.customer_id, ci.full_name, ci.value_segment;

-- COMMAND ----------

SELECT * FROM goldenlayer.customer_satisfaction_index

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS goldenlayer.customer_brand_preference
USING DELTA
AS
SELECT
    di.brand_name,
    COUNT(DISTINCT ci.customer_id) AS customer_count,
    COUNT(DISTINCT di.imei_tac) AS device_count,
    COUNT(DISTINCT bi.billing_id) AS total_transactions,
    SUM(bi.bill_amount) AS total_billing_amount,
    AVG(cr.rating) AS avg_rating,
    COUNT(DISTINCT cr.customer_id) AS rated_customers_count,
    COUNT(DISTINCT bi.payment_date) AS days_active
FROM capstone.device_information di
LEFT JOIN capstone.customer_information ci ON di.customer_id = ci.customer_id
LEFT JOIN capstone.billing_information bi ON di.customer_id = bi.customer_id
LEFT JOIN capstone.customer_rating cr ON di.customer_id = cr.customer_id
GROUP BY di.brand_name;

-- COMMAND ----------


