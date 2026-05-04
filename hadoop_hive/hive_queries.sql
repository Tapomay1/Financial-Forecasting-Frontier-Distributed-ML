-- ============================================================================
-- HIVE SCRIPT: Banking Data Analysis (Refactored Version)
-- Run inside HiveServer2 using Beeline
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- 1. DATABASE SETUP & DATA INGESTION
-- ────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS banking_data;

USE banking_data;

-- Create main table for client dataset
CREATE TABLE IF NOT EXISTS client_info (
    age         INT,
    job         STRING,
    marital     STRING,
    education   STRING,
    default_    STRING,
    balance     INT,
    housing     STRING,
    loan        STRING,
    contact     STRING,
    day         INT,
    month       STRING,
    duration    INT,
    campaign    INT,
    pdays       INT,
    previous    INT,
    poutcome    STRING,
    y           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count" = "1");


-- Load dataset from HDFS
-- (Ensure file exists: /user/hive/bank.csv)
LOAD DATA INPATH '/user/hive/bank.csv'
INTO TABLE client_info;



-- ────────────────────────────────────────────────────────────────────────────
-- 2. BASIC DATA EXPLORATION
-- ────────────────────────────────────────────────────────────────────────────

-- Total number of records
SELECT
    COUNT(*) AS total_clients
FROM client_info;


-- Preview sample data
SELECT *
FROM client_info
LIMIT 10;



-- ────────────────────────────────────────────────────────────────────────────
-- 3. FILTERING & SORTING
-- ────────────────────────────────────────────────────────────────────────────

-- Married clients who have an active loan
SELECT *
FROM client_info
WHERE marital = 'married'
  AND loan = 'yes';


-- Top 10 clients by account balance
SELECT
    job,
    marital,
    balance
FROM client_info
ORDER BY balance DESC
LIMIT 10;



-- ────────────────────────────────────────────────────────────────────────────
-- 4. AGGREGATION & GROUPING
-- ────────────────────────────────────────────────────────────────────────────

-- Average age across job categories
SELECT
    job,
    ROUND(AVG(age), 2) AS avg_age
FROM client_info
GROUP BY job
ORDER BY avg_age DESC;


-- Count of defaulted clients per education level
SELECT
    education,
    COUNT(*) AS default_count
FROM client_info
WHERE default_ = 'yes'
GROUP BY education
ORDER BY default_count DESC;



-- ────────────────────────────────────────────────────────────────────────────
-- 5. BUSINESS INSIGHTS (COMPLEX QUERIES)
-- ────────────────────────────────────────────────────────────────────────────

-- Top 5 job roles by average balance + subscription success rate
SELECT
    job,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(
        SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS subscription_rate_pct
FROM client_info
GROUP BY job
ORDER BY avg_balance DESC
LIMIT 5;


-- Month with highest engagement + campaign success rate
SELECT
    month,
    COUNT(*) AS total_contacts,
    ROUND(
        SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS success_rate_pct
FROM client_info
GROUP BY month
ORDER BY total_contacts DESC
LIMIT 1;



-- ────────────────────────────────────────────────────────────────────────────
-- 6. CORRELATION ANALYSIS
-- ────────────────────────────────────────────────────────────────────────────

-- Relationship between age and account balance
SELECT
    CORR(age, balance) AS age_balance_corr
FROM client_info;



-- ────────────────────────────────────────────────────────────────────────────
-- 7. TEMPORAL TREND ANALYSIS
-- ────────────────────────────────────────────────────────────────────────────

-- Monthly contact distribution (ordered chronologically)
SELECT
    month,
    COUNT(*) AS contact_volume
FROM client_info
GROUP BY month
ORDER BY
    CASE month
        WHEN 'jan' THEN 1 WHEN 'feb' THEN 2 WHEN 'mar' THEN 3
        WHEN 'apr' THEN 4 WHEN 'may' THEN 5 WHEN 'jun' THEN 6
        WHEN 'jul' THEN 7 WHEN 'aug' THEN 8 WHEN 'sep' THEN 9
        WHEN 'oct' THEN 10 WHEN 'nov' THEN 11 WHEN 'dec' THEN 12
    END;



-- ────────────────────────────────────────────────────────────────────────────
-- 8. ANOMALY & VARIABILITY ANALYSIS
-- ────────────────────────────────────────────────────────────────────────────

-- Balance distribution metrics by education category
SELECT
    education,
    ROUND(AVG(balance), 2)     AS avg_balance,
    ROUND(STDDEV(balance), 2)  AS stddev_balance,
    MIN(balance)               AS min_balance,
    MAX(balance)               AS max_balance
FROM client_info
GROUP BY education
ORDER BY avg_balance DESC;



-- ────────────────────────────────────────────────────────────────────────────
-- 9. ADVANCED ANALYTICS
-- ────────────────────────────────────────────────────────────────────────────

-- Effect of previous campaign outcomes on current subscription
SELECT
    poutcome,
    COUNT(*) AS total_clients,
    SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) AS subscribed_clients,
    ROUND(
        SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS subscription_rate_pct
FROM client_info
GROUP BY poutcome
ORDER BY subscription_rate_pct DESC;


-- Average call duration grouped by subscription outcome
SELECT
    y AS subscription_status,
    ROUND(AVG(duration), 2) AS avg_call_duration
FROM client_info
GROUP BY y;



-- ============================================================================
-- END OF SCRIPT
-- ============================================================================