-- ============================================================
-- HIVE SCRIPTS - Banking Data Analysis
-- Run these inside hiveserver2 container:
--   docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
-- ============================================================

-- ─── 1. Data Ingestion and Table Creation ────────────────────────────

CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

CREATE TABLE IF NOT EXISTS client_info (
    age        INT,
    job        STRING,
    marital    STRING,
    education  STRING,
    default_   STRING,
    balance    INT,
    housing    STRING,
    loan       STRING,
    contact    STRING,
    day        INT,
    month      STRING,
    duration   INT,
    campaign   INT,
    pdays      INT,
    previous   INT,
    poutcome   STRING,
    y          STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data from HDFS (after uploading with: hdfs dfs -put /data/bank.csv /user/hive/)
LOAD DATA INPATH '/user/hive/bank.csv' INTO TABLE client_info;


-- ─── 2. Basic Data Exploration ───────────────────────────────────────

-- Count total number of clients
SELECT COUNT(*) AS total_clients FROM client_info;

-- Display first 10 rows
SELECT * FROM client_info LIMIT 10;


-- ─── 3. Data Filtering and Sorting ───────────────────────────────────

-- Married clients with a personal loan
SELECT * FROM client_info
WHERE marital = 'married' AND loan = 'yes';

-- Top 10 clients with highest balance
SELECT job, marital, balance
FROM client_info
ORDER BY balance DESC
LIMIT 10;


-- ─── 4. Data Aggregation and Grouping ────────────────────────────────

-- Average age per job category
SELECT job, ROUND(AVG(age), 2) AS avg_age
FROM client_info
GROUP BY job
ORDER BY avg_age DESC;

-- Total clients per education level who defaulted
SELECT education, COUNT(*) AS default_count
FROM client_info
WHERE default_ = 'yes'
GROUP BY education
ORDER BY default_count DESC;


-- ─── 5. Complex Queries for Insights ─────────────────────────────────

-- Top 5 job categories with highest average balance + subscription %
SELECT
    job,
    ROUND(AVG(balance), 2)                                        AS avg_balance,
    ROUND(SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2)                                          AS subscription_pct
FROM client_info
GROUP BY job
ORDER BY avg_balance DESC
LIMIT 5;

-- Month with highest contacts + campaign success rate
SELECT
    month,
    COUNT(*)                                                      AS total_contacts,
    ROUND(SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2)                                         AS success_rate_pct
FROM client_info
GROUP BY month
ORDER BY total_contacts DESC
LIMIT 1;


-- ─── 6. Correlation Analysis ─────────────────────────────────────────

-- Correlation between age and balance
SELECT
    CORR(age, balance) AS age_balance_correlation
FROM client_info;


-- ─── 7. Trend Analysis ───────────────────────────────────────────────

-- Contacts per month (proxy for year-over-year trend using month ordering)
SELECT month, COUNT(*) AS contacts_count
FROM client_info
GROUP BY month
ORDER BY
    CASE month
        WHEN 'jan' THEN 1 WHEN 'feb' THEN 2 WHEN 'mar' THEN 3
        WHEN 'apr' THEN 4 WHEN 'may' THEN 5 WHEN 'jun' THEN 6
        WHEN 'jul' THEN 7 WHEN 'aug' THEN 8 WHEN 'sep' THEN 9
        WHEN 'oct' THEN 10 WHEN 'nov' THEN 11 WHEN 'dec' THEN 12
    END;


-- ─── 8. Anomaly Detection ────────────────────────────────────────────

-- Average yearly balance across education levels (flag outliers)
SELECT
    education,
    ROUND(AVG(balance), 2)  AS avg_balance,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    MIN(balance)             AS min_balance,
    MAX(balance)             AS max_balance
FROM client_info
GROUP BY education
ORDER BY avg_balance DESC;


-- ─── 9. Advanced Analysis ────────────────────────────────────────────

-- Impact of previous campaign outcome on current subscription rate
SELECT
    poutcome,
    COUNT(*)                                                      AS total,
    SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END)                   AS subscribed,
    ROUND(SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2)                                          AS subscription_rate_pct
FROM client_info
GROUP BY poutcome
ORDER BY subscription_rate_pct DESC;

-- Average contact duration: subscribed vs not subscribed
SELECT
    y                           AS subscribed,
    ROUND(AVG(duration), 2)     AS avg_duration_secs
FROM client_info
GROUP BY y;
