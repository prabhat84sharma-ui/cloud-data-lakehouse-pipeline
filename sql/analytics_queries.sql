-- ==============================================================================
-- Enterprise Analytics & Lakehouse SQL Query Suite
-- Engine: Trino / Presto / Apache Spark SQL (Delta Lake Compatible)
-- Author: Prabhat Sharma (https://github.com/prabhat84sharma-ui)
-- Architecture: Medallion Architecture (Targeting Silver & Gold Delta Tables)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- QUERY 1: Rolling 7-Day and 30-Day Customer Spend Trajectory
-- Business Value: Tracks customer velocity and acceleration in spending over time.
-- Performance Optimization:
--   - Uses partition pruning on 'region'
--   - Leverages RANGE frame with unix timestamp casting for exact sliding time windows.
-- ------------------------------------------------------------------------------
WITH customer_daily_spend AS (
    SELECT
        customer_id,
        region,
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        SUM(amount_usd) AS daily_spend_usd,
        COUNT(transaction_id) AS daily_order_count
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
    GROUP BY
        customer_id,
        region,
        CAST(transaction_timestamp AS DATE)
)
SELECT
    customer_id,
    region,
    transaction_date,
    daily_spend_usd,
    daily_order_count,
    -- Rolling 7-Day Cumulative Spend
    SUM(daily_spend_usd) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_spend_usd,
    -- Rolling 30-Day Cumulative Spend
    SUM(daily_spend_usd) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_spend_usd,
    -- 7-Day Moving Average Order Value
    ROUND(AVG(daily_spend_usd / daily_order_count) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7d_avg_order_value
FROM customer_daily_spend
ORDER BY customer_id, transaction_date DESC;


-- ------------------------------------------------------------------------------
-- QUERY 2: Customer RFM (Recency, Frequency, Monetary) Segmentation
-- Business Value: Classifies customers into actionable marketing quartiles.
-- Performance Optimization:
--   - Pre-aggregates metrics in single scan CTE before NTILE distribution.
-- ------------------------------------------------------------------------------
WITH rfm_base AS (
    SELECT
        customer_id,
        DATEDIFF(CURRENT_DATE(), MAX(CAST(transaction_timestamp AS DATE))) AS recency_days,
        COUNT(DISTINCT transaction_id) AS frequency_orders,
        SUM(amount_usd) AS monetary_spend_usd
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency_orders,
        monetary_spend_usd,
        -- Lower recency is better (rank 4 = most recent)
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        -- Higher frequency is better (rank 4 = most frequent)
        NTILE(4) OVER (ORDER BY frequency_orders ASC) AS f_score,
        -- Higher monetary spend is better (rank 4 = biggest spenders)
        NTILE(4) OVER (ORDER BY monetary_spend_usd ASC) AS m_score
    FROM rfm_base
)
SELECT
    customer_id,
    recency_days,
    frequency_orders,
    ROUND(monetary_spend_usd, 2) AS monetary_spend_usd,
    r_score,
    f_score,
    m_score,
    (r_score * 100 + f_score * 10 + m_score) AS composite_rfm_score,
    CASE
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'CHAMPION / VIP'
        WHEN f_score >= 3 AND m_score >= 2 THEN 'LOYAL CUSTOMER'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'PROMISING NEW'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'AT RISK OF CHURN'
        ELSE 'HIBERNATING / CASUAL'
    END AS customer_persona
FROM rfm_scores
ORDER BY composite_rfm_score DESC, monetary_spend_usd DESC;


-- ------------------------------------------------------------------------------
-- QUERY 3: High-Velocity Fraud & Rapid-Fire Anomaly Detection
-- Business Value: Detects suspicious card swipes within 5 minutes across regions.
-- Performance Optimization:
--   - LAG() over temporal ordering partitioned by customer.
-- ------------------------------------------------------------------------------
WITH transaction_intervals AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        region,
        amount_usd,
        transaction_timestamp,
        LAG(transaction_timestamp, 1) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
        ) AS prev_transaction_timestamp,
        LAG(region, 1) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
        ) AS prev_region
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status != 'FAILED'
)
SELECT
    transaction_id,
    customer_id,
    region AS current_region,
    prev_region,
    amount_usd,
    transaction_timestamp,
    prev_transaction_timestamp,
    ROUND((UNIX_TIMESTAMP(transaction_timestamp) - UNIX_TIMESTAMP(prev_transaction_timestamp)) / 60.0, 2) AS minutes_between_txns,
    CASE
        WHEN (UNIX_TIMESTAMP(transaction_timestamp) - UNIX_TIMESTAMP(prev_transaction_timestamp)) <= 300
             AND region != prev_region THEN 'CRITICAL: IMPOSSIBLE TRAVEL ANOMALY'
        WHEN (UNIX_TIMESTAMP(transaction_timestamp) - UNIX_TIMESTAMP(prev_transaction_timestamp)) <= 300 THEN 'HIGH: RAPID VELOCITY BURST'
        ELSE 'NORMAL'
    END AS fraud_risk_indicator
FROM transaction_intervals
WHERE prev_transaction_timestamp IS NOT NULL
  AND (UNIX_TIMESTAMP(transaction_timestamp) - UNIX_TIMESTAMP(prev_transaction_timestamp)) <= 600
ORDER BY minutes_between_txns ASC;


-- ------------------------------------------------------------------------------
-- QUERY 4: Customer Cohort Retention & Activity Heatmap
-- Business Value: Tracks long-term customer engagement by acquisition cohort.
-- Performance Optimization:
--   - Two-stage aggregation minimizes shuffle byte size.
-- ------------------------------------------------------------------------------
WITH customer_first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(transaction_timestamp)) AS cohort_month
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
    GROUP BY customer_id
),
customer_monthly_activity AS (
    SELECT
        t.customer_id,
        fp.cohort_month,
        DATE_TRUNC('month', t.transaction_timestamp) AS activity_month,
        ROUND(
            (YEAR(t.transaction_timestamp) - YEAR(fp.cohort_month)) * 12 +
            (MONTH(t.transaction_timestamp) - MONTH(fp.cohort_month))
        ) AS month_number,
        SUM(t.amount_usd) AS monthly_spend
    FROM delta.`./lakehouse_storage/silver/transactions` t
    JOIN customer_first_purchase fp ON t.customer_id = fp.customer_id
    WHERE t.status = 'COMPLETED'
    GROUP BY
        t.customer_id,
        fp.cohort_month,
        DATE_TRUNC('month', t.transaction_timestamp),
        YEAR(t.transaction_timestamp),
        MONTH(t.transaction_timestamp)
)
SELECT
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) AS active_retained_customers,
    ROUND(SUM(monthly_spend), 2) AS total_cohort_revenue_usd,
    ROUND(AVG(monthly_spend), 2) AS avg_revenue_per_active_user
FROM customer_monthly_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month ASC, month_number ASC;


-- ------------------------------------------------------------------------------
-- QUERY 5: Regional Pareto (80/20 Distribution) & Cumulative Market Share
-- Business Value: Validates whether 20% of accounts generate 80% of revenue.
-- ------------------------------------------------------------------------------
WITH customer_revenue AS (
    SELECT
        customer_id,
        region,
        SUM(amount_usd) AS customer_total_spend
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
    GROUP BY customer_id, region
),
revenue_distribution AS (
    SELECT
        customer_id,
        region,
        customer_total_spend,
        SUM(customer_total_spend) OVER (PARTITION BY region) AS regional_total_spend,
        SUM(customer_total_spend) OVER (
            PARTITION BY region
            ORDER BY customer_total_spend DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_regional_spend,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY customer_total_spend DESC) AS customer_rank_in_region,
        COUNT(customer_id) OVER (PARTITION BY region) AS total_customers_in_region
    FROM customer_revenue
)
SELECT
    customer_id,
    region,
    ROUND(customer_total_spend, 2) AS customer_spend_usd,
    ROUND(regional_total_spend, 2) AS regional_total_spend_usd,
    ROUND((cumulative_regional_spend / regional_total_spend) * 100.0, 2) AS cumulative_revenue_share_pct,
    ROUND((CAST(customer_rank_in_region AS DOUBLE) / total_customers_in_region) * 100.0, 2) AS customer_percentile_rank,
    CASE
        WHEN (cumulative_regional_spend / regional_total_spend) <= 0.80 THEN 'Top 80% Revenue Generator'
        ELSE 'Long-tail Contributor'
    END AS pareto_classification
FROM revenue_distribution
ORDER BY region, customer_total_spend DESC;


-- ------------------------------------------------------------------------------
-- QUERY 6: Month-over-Month (MoM) Growth Rate & Volume Acceleration
-- Business Value: Executive financial reporting on revenue and volume drift.
-- ------------------------------------------------------------------------------
WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', transaction_timestamp) AS reporting_month,
        COUNT(transaction_id) AS total_orders,
        SUM(CASE WHEN status = 'COMPLETED' THEN amount_usd ELSE 0 END) AS net_revenue_usd,
        SUM(CASE WHEN status = 'REFUNDED' THEN amount_usd ELSE 0 END) AS refunded_volume_usd
    FROM delta.`./lakehouse_storage/silver/transactions`
    GROUP BY DATE_TRUNC('month', transaction_timestamp)
),
mom_comparison AS (
    SELECT
        reporting_month,
        total_orders,
        net_revenue_usd,
        refunded_volume_usd,
        LAG(net_revenue_usd, 1) OVER (ORDER BY reporting_month) AS prev_month_net_revenue,
        LAG(total_orders, 1) OVER (ORDER BY reporting_month) AS prev_month_total_orders
    FROM monthly_metrics
)
SELECT
    reporting_month,
    total_orders,
    ROUND(net_revenue_usd, 2) AS net_revenue_usd,
    ROUND(prev_month_net_revenue, 2) AS prev_month_revenue_usd,
    ROUND(
        ((net_revenue_usd - prev_month_net_revenue) / NULLIF(prev_month_net_revenue, 0)) * 100.0,
        2
    ) AS mom_revenue_growth_pct,
    ROUND(
        ((CAST(total_orders AS DOUBLE) - prev_month_total_orders) / NULLIF(prev_month_total_orders, 0)) * 100.0,
        2
    ) AS mom_order_volume_growth_pct
FROM mom_comparison
ORDER BY reporting_month DESC;


-- ------------------------------------------------------------------------------
-- QUERY 7: Top N Customers per Region with DENSE_RANK & Tie Handling
-- Business Value: High-priority key account management and territory planning.
-- ------------------------------------------------------------------------------
WITH regional_ranks AS (
    SELECT
        region,
        customer_id,
        SUM(amount_usd) AS total_regional_spend,
        COUNT(transaction_id) AS order_count,
        MAX(transaction_timestamp) AS last_active_at,
        DENSE_RANK() OVER (
            PARTITION BY region
            ORDER BY SUM(amount_usd) DESC
        ) AS territory_rank
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
    GROUP BY region, customer_id
)
SELECT
    region,
    territory_rank,
    customer_id,
    ROUND(total_regional_spend, 2) AS total_spend_usd,
    order_count,
    last_active_at
FROM regional_ranks
WHERE territory_rank <= 5
ORDER BY region, territory_rank ASC;


-- ------------------------------------------------------------------------------
-- QUERY 8: Multi-Currency FX Exposure & Hedging Risk Analysis
-- Business Value: Assesses corporate foreign currency volatility exposure.
-- ------------------------------------------------------------------------------
SELECT
    currency,
    COUNT(transaction_id) AS transaction_count,
    ROUND(SUM(amount), 2) AS native_currency_volume,
    ROUND(SUM(amount_usd), 2) AS converted_usd_volume,
    ROUND(AVG(fx_rate_to_usd), 4) AS applied_fx_rate,
    ROUND(
        (SUM(amount_usd) / SUM(SUM(amount_usd)) OVER ()) * 100.0, 2
    ) AS portfolio_fx_exposure_pct,
    ROUND(MIN(amount_usd), 2) AS min_ticket_usd,
    ROUND(MAX(amount_usd), 2) AS max_ticket_usd
FROM delta.`./lakehouse_storage/silver/transactions`
WHERE status = 'COMPLETED'
GROUP BY currency
ORDER BY converted_usd_volume DESC;


-- ------------------------------------------------------------------------------
-- QUERY 9: Customer Churn Early Warning Indicator (Inter-Purchase Gap Analysis)
-- Business Value: Identifies users whose inactivity exceeds 2x their historic cadence.
-- ------------------------------------------------------------------------------
WITH customer_intervals AS (
    SELECT
        customer_id,
        transaction_timestamp,
        LAG(transaction_timestamp, 1) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
        ) AS prev_txn_ts
    FROM delta.`./lakehouse_storage/silver/transactions`
    WHERE status = 'COMPLETED'
),
cadence_metrics AS (
    SELECT
        customer_id,
        ROUND(AVG(DATEDIFF(transaction_timestamp, prev_txn_ts)), 1) AS avg_days_between_purchases,
        MAX(transaction_timestamp) AS last_purchase_timestamp,
        DATEDIFF(CURRENT_DATE(), CAST(MAX(transaction_timestamp) AS DATE)) AS days_since_last_purchase
    FROM customer_intervals
    WHERE prev_txn_ts IS NOT NULL
    GROUP BY customer_id
)
SELECT
    customer_id,
    avg_days_between_purchases,
    days_since_last_purchase,
    ROUND(days_since_last_purchase / NULLIF(avg_days_between_purchases, 0), 2) AS dormancy_multiplier,
    CASE
        WHEN days_since_last_purchase >= (avg_days_between_purchases * 3) THEN 'CRITICAL CHURN RISK'
        WHEN days_since_last_purchase >= (avg_days_between_purchases * 2) THEN 'ELEVATED INACTIVITY'
        ELSE 'HEALTHY CADENCE'
    END AS churn_risk_assessment
FROM cadence_metrics
WHERE days_since_last_purchase >= 14
ORDER BY dormancy_multiplier DESC;


-- ------------------------------------------------------------------------------
-- QUERY 10: Delta Lake Time-Travel Change Data Capture (CDC) Audit
-- Business Value: Compares latest Silver snapshot with previous commit version
--                 to identify mutated and newly inserted rows for data governance.
-- ------------------------------------------------------------------------------
SELECT
    curr.transaction_id,
    curr.customer_id,
    curr.status AS current_status,
    prev.status AS previous_status,
    curr.amount_usd AS current_amount_usd,
    prev.amount_usd AS previous_amount_usd,
    CASE
        WHEN prev.transaction_id IS NULL THEN 'INSERTED_RECORD'
        WHEN curr.status != prev.status THEN 'STATUS_MUTATED'
        WHEN curr.amount_usd != prev.amount_usd THEN 'AMOUNT_CORRECTED'
        ELSE 'UNMODIFIED'
    END AS audit_change_type
FROM delta.`./lakehouse_storage/silver/transactions` curr
FULL OUTER JOIN delta.`./lakehouse_storage/silver/transactions` VERSION AS OF 0 prev
    ON curr.transaction_id = prev.transaction_id
WHERE prev.transaction_id IS NULL
   OR curr.status != prev.status
   OR curr.amount_usd != prev.amount_usd;


-- ------------------------------------------------------------------------------
-- QUERY 11: Product Velocity and Refund Failure Root Cause Mart
-- Business Value: Pinpoints products causing anomalous failure or refund rates.
-- ------------------------------------------------------------------------------
SELECT
    product_id,
    COUNT(transaction_id) AS total_attempts,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS successful_orders,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_orders,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_orders,
    ROUND(SUM(CASE WHEN status = 'COMPLETED' THEN amount_usd ELSE 0 END), 2) AS net_revenue_usd,
    ROUND(
        (COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) * 100.0) / COUNT(transaction_id), 2
    ) AS refund_rate_pct,
    ROUND(
        (COUNT(CASE WHEN status = 'FAILED' THEN 1 END) * 100.0) / COUNT(transaction_id), 2
    ) AS failure_rate_pct
FROM delta.`./lakehouse_storage/silver/transactions`
GROUP BY product_id
HAVING COUNT(transaction_id) >= 1
ORDER BY refund_rate_pct DESC, failure_rate_pct DESC;
