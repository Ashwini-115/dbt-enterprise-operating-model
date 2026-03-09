-- MART: mart_gl_detail
-- Purpose: Full GL transaction detail enriched with all lookup tables
-- Transformations: Multi-table JOINs, Lookup enrichment,
--                  CASE WHEN classification, Currency conversion
-- Materialization: TABLE (full refresh each run)

{{
    config(
        materialized='table',
        tags=['finance', 'gl', 'daily']
    )
}}

WITH gl AS (
    SELECT * FROM {{ ref('stg_gl_transactions') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_chart_of_accounts') }}
),

cost_centers AS (
    SELECT * FROM {{ ref('stg_cost_centers') }}
),

vendors AS (
    SELECT * FROM {{ ref('stg_vendors') }}
),

currency_rates AS (
    SELECT * FROM {{ ref('stg_currency_rates') }}
),

-- ── JOIN all lookups onto GL transactions ─────────────────────────────────
enriched AS (
    SELECT
        -- Transaction identifiers
        gl.transaction_id,
        gl.journal_id,
        gl.transaction_date,
        gl.fiscal_year,
        gl.fiscal_period,
        {{ generate_fiscal_label('gl.fiscal_year', 'gl.fiscal_period') }}
                                                AS fiscal_label,

        -- Account dimension (lookup JOIN)
        gl.account_code,
        acc.account_name,
        acc.account_type,
        acc.account_category,
        acc.normal_balance,

        -- Account classification via macro (CASE WHEN)
        {{ classify_account_type('acc.account_type') }}
                                                AS financial_statement,
        {{ classify_account_subtype(
            'acc.account_type',
            'acc.normal_balance',
            'gl.debit_amount - gl.credit_amount'
        ) }}                                    AS account_subtype,

        -- Cost center dimension (lookup JOIN)
        gl.cost_center_code,
        cc.cost_center_name,
        cc.department,
        cc.region,
        cc.manager_name,

        -- Vendor dimension (lookup JOIN — left join as not all entries have vendors)
        gl.vendor_id,
        vnd.vendor_name,
        vnd.vendor_type,
        vnd.country                             AS vendor_country,
        vnd.payment_terms,

        -- Amounts in original currency
        gl.currency_code,
        gl.debit_amount,
        gl.credit_amount,
        gl.debit_amount - gl.credit_amount      AS net_amount,

        -- Amounts converted to USD via macro (currency lookup JOIN)
        fx.rate_to_usd,
        {{ convert_to_usd('gl.debit_amount', 'fx.rate_to_usd') }}
                                                AS debit_amount_usd,
        {{ convert_to_usd('gl.credit_amount', 'fx.rate_to_usd') }}
                                                AS credit_amount_usd,
        {{ convert_to_usd('gl.debit_amount - gl.credit_amount', 'fx.rate_to_usd') }}
                                                AS net_amount_usd,

        -- Descriptive
        gl.description,
        gl.status

    FROM gl

    -- Account lookup — INNER JOIN (every transaction must have a valid account)
    INNER JOIN accounts acc
        ON gl.account_code = acc.account_code

    -- Cost center lookup — LEFT JOIN (some entries may not have cost center)
    LEFT JOIN cost_centers cc
        ON gl.cost_center_code = cc.cost_center_code

    -- Vendor lookup — LEFT JOIN (non-vendor entries will be NULL)
    LEFT JOIN vendors vnd
        ON gl.vendor_id = vnd.vendor_id

    -- Currency rate lookup — LEFT JOIN with fallback to 1.0 for USD
    LEFT JOIN currency_rates fx
        ON gl.currency_code = fx.currency_code
)

SELECT * FROM enriched