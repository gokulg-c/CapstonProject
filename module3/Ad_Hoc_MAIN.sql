-- Databricks notebook source
CREATE TABLE IF NOT EXISTS ad_hoc.customers_with_multiple_devices
USING DELTA
AS
SELECT
    ci.customer_id,
    COUNT(DISTINCT di.imei_tac) AS device_count,
    SUM(bi.bill_amount) AS total_bill_amount
FROM
    customer_information ci
JOIN
    device_information di ON ci.customer_id = di.customer_id
JOIN
    billing_information bi ON ci.customer_id = bi.customer_id
GROUP BY
    ci.customer_id
HAVING
    COUNT(DISTINCT di.imei_tac) > 1;

-- COMMAND ----------

select * from ad_hoc.customers_with_multiple_devices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2nd

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ad_hoc.customer_billing_summary
USING DELTA
AS
SELECT
    customer_id,
    COUNT(*) AS number_of_bills,
    SUM(bill_amount) AS total_bill_amount,
    AVG(bill_amount) AS average_bill_amount
FROM
    Billing_Information
GROUP BY
    customer_id;

-- COMMAND ----------

select * from ad_hoc.customer_billing_summary

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##3rd

-- COMMAND ----------

SELECT
    brand_name,
    COUNT(CASE WHEN os_name = 'Android' THEN 1 ELSE NULL END) AS Android,
    COUNT(CASE WHEN os_name = 'Google' THEN 1 ELSE NULL END) AS Google,
    COUNT(CASE WHEN os_name = 'LG OS' THEN 1 ELSE NULL END) AS LG_OS,
    COUNT(CASE WHEN os_name = 'Samsung OS' THEN 1 ELSE NULL END) AS Samsung_OS,
    COUNT(CASE WHEN os_name = 'Proprietary OS' THEN 1 ELSE NULL END) AS Proprietary_OS
FROM
    Device_Information
GROUP BY
    brand_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##4th

-- COMMAND ----------

WITH CustomerBilling AS (
    SELECT
        customer_id,
        billing_date,
        bill_amount,
        AVG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_bill_amount,
        LAG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date) AS previous_bill_amount
    FROM
        Billing_Information
)
SELECT
    customer_id,
    billing_date,
    bill_amount,
    COALESCE(avg_bill_amount, bill_amount) AS avg_bill_amount,
    previous_bill_amount
FROM
    CustomerBilling
ORDER BY
    customer_id, billing_date;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##5th

-- COMMAND ----------

WITH Customerbill AS (
    SELECT
        customer_id,
        COUNT(*) AS bill_count,
        AVG(bill_amount) AS avg_bill_amount
    FROM
        billing_information 
    GROUP BY
        customer_id
),
AvgbillCount AS (
    SELECT
        AVG(bill_count) AS avg_bill_count
    FROM
        Customerbill
)
SELECT
    cp.customer_id,
    cp.bill_count,
    cp.avg_bill_amount
FROM
    Customerbill cp
JOIN
    AvgbillCount ap ON cp.bill_count > ap.avg_bill_count
ORDER BY
    cp.customer_id;


-- COMMAND ----------


