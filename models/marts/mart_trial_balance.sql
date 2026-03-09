-- MART: mart_trial_balance
-- Purpose: Aggregated trial balance by account and fiscal period
-- Transformations: Aggregations (SUM), CASE WHEN net amount logic,
--                  account classification
-- Materialization: TABLE

{{
    config(
        materialized='table',
        tags=['finance', 'trial_balance', 'monthly']
    )
}}

WITH gl_detail AS (
    SELECT * FROM {{ ref('mart_gl_detail') }}
),

-- ── Aggregate to account + period level ───────────────────────────────────
aggregated AS (
    SELECT
        fiscal_year,
        fiscal_period,
        fiscal_label,
        account_code,
        account_name,
        account_type,
        account_category,
        normal_balance,
        financial_statement,
        account_subtype,

        -- Aggregations
        SUM(debit_amount_usd)                           AS total_debit_usd,
        SUM(credit_amount_usd)                          AS total_credit_usd,
        COUNT(DISTINCT transaction_id)                  AS transaction_count,
        COUNT(DISTINCT journal_id)                      AS journal_count

    FROM gl_detail
    GROUP BY
        fiscal_year,
        fiscal_period,
        fiscal_label,
        account_code,
        account_name,
        account_type,
        account_category,
        normal_balance,
        financial_statement,
        account_subtype
),

-- ── Apply net amount logic via macro ──────────────────────────────────────
with_net AS (
    SELECT
        *,
        -- Net amount respects normal balance direction (macro)
        {{ get_net_amount('total_debit_usd', 'total_credit_usd', 'normal_balance') }}
                                                        AS net_balance_usd,

        -- Running balance flag for reporting
        CASE
            WHEN {{ get_net_amount('total_debit_usd', 'total_credit_usd', 'normal_balance') }} > 0
                THEN 'Positive Balance'
            WHEN {{ get_net_amount('total_debit_usd', 'total_credit_usd', 'normal_balance') }} < 0
                THEN 'Negative Balance — Review Required'
            ELSE
                'Zero Balance'
        END                                             AS balance_status,

        -- Is this balance sheet or P&L
        CASE
            WHEN financial_statement = 'Balance Sheet'  THEN total_debit_usd - total_credit_usd
            ELSE NULL
        END                                             AS bs_net_usd,

        CASE
            WHEN financial_statement = 'Profit & Loss'  THEN total_credit_usd - total_debit_usd
            ELSE NULL
        END                                             AS pl_net_usd

    FROM aggregated
)

SELECT * FROM with_net
ORDER BY fiscal_year, fiscal_period, account_code