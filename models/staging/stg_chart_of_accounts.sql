-- Staging for Chart of Accounts lookup table

WITH source AS (
    SELECT * FROM {{ source('raw', 'chart_of_accounts') }}
)

SELECT
    UPPER(TRIM(account_code))               AS account_code,
    TRIM(account_name)                      AS account_name,
    INITCAP(TRIM(account_type))             AS account_type,
    TRIM(account_category)                  AS account_category,
    UPPER(TRIM(normal_balance))             AS normal_balance,
    is_active,
    created_at
FROM source
WHERE is_active = TRUE