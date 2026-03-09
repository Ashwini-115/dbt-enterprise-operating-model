-- Staging model for GL transactions
-- Casts data types, renames columns, filters out invalid records

WITH source AS (
    SELECT * FROM {{ source('raw', 'gl_transactions') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        transaction_id                              AS transaction_id,
        journal_id                                  AS journal_id,

        -- Foreign keys to lookup tables
        UPPER(TRIM(account_code))                   AS account_code,
        UPPER(TRIM(cost_center_code))               AS cost_center_code,
        vendor_id                                   AS vendor_id,
        UPPER(TRIM(currency_code))                  AS currency_code,

        -- Amounts — ensure nulls become 0
        COALESCE(debit_amount, 0)                   AS debit_amount,
        COALESCE(credit_amount, 0)                  AS credit_amount,

        -- Dates
        CAST(transaction_date AS DATE)              AS transaction_date,
        fiscal_year,
        fiscal_period,

        -- Descriptive fields
        TRIM(description)                           AS description,
        UPPER(TRIM(status))                         AS status,

        -- Audit
        created_at
    FROM source
    WHERE
        transaction_id IS NOT NULL
        AND status = 'POSTED'           -- only include posted entries
)

SELECT * FROM cleaned