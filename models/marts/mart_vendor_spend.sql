-- MART: mart_vendor_spend
-- Purpose: Vendor spend analysis by department and period
-- Transformations: Aggregations, CASE WHEN spend tier classification,
--                  multi-dimension grouping
-- Materialization: TABLE

{{
    config(
        materialized='table',
        tags=['finance', 'vendor', 'monthly']
    )
}}

WITH gl_detail AS (
    -- Only take expense entries with a vendor
    SELECT *
    FROM {{ ref('mart_gl_detail') }}
    WHERE
        account_type = 'Expense'
        AND vendor_id IS NOT NULL
),

aggregated AS (
    SELECT
        fiscal_year,
        fiscal_period,
        fiscal_label,

        -- Vendor details
        vendor_id,
        vendor_name,
        vendor_type,
        vendor_country,
        payment_terms,

        -- Department details
        department,
        region,

        -- Aggregations
        SUM(debit_amount_usd)                       AS total_spend_usd,
        COUNT(DISTINCT transaction_id)              AS transaction_count,
        COUNT(DISTINCT account_code)                AS accounts_used,
        AVG(debit_amount_usd)                       AS avg_transaction_usd,
        MIN(debit_amount_usd)                       AS min_transaction_usd,
        MAX(debit_amount_usd)                       AS max_transaction_usd

    FROM gl_detail
    GROUP BY
        fiscal_year,
        fiscal_period,
        fiscal_label,
        vendor_id,
        vendor_name,
        vendor_type,
        vendor_country,
        payment_terms,
        department,
        region
),

-- ── Enrich with spend tier classification ────────────────────────────────
classified AS (
    SELECT
        *,
        -- Spend tier CASE WHEN classification
        CASE
            WHEN total_spend_usd >= 100000  THEN 'Tier 1 — Strategic'
            WHEN total_spend_usd >= 25000   THEN 'Tier 2 — Preferred'
            WHEN total_spend_usd >= 5000    THEN 'Tier 3 — Standard'
            ELSE                                 'Tier 4 — Spot'
        END                                         AS vendor_spend_tier,

        -- Payment risk flag
        CASE
            WHEN payment_terms = 'NET-60'
                 AND total_spend_usd > 50000        THEN 'High Exposure'
            WHEN payment_terms = 'NET-45'
                 AND total_spend_usd > 25000        THEN 'Medium Exposure'
            ELSE                                         'Low Exposure'
        END                                         AS payment_risk_flag,

        -- Spend concentration (% of total period spend — window function)
        ROUND(
            100.0 * total_spend_usd /
            SUM(total_spend_usd) OVER (
                PARTITION BY fiscal_year, fiscal_period
            ),
            2
        )                                           AS pct_of_period_spend

    FROM aggregated
)

SELECT * FROM classified
ORDER BY fiscal_year, fiscal_period, total_spend_usd DESC