-- Staging for Currency Rates
-- Gets the most recent rate per currency using ROW_NUMBER

WITH source AS (
    SELECT * FROM {{ source('raw', 'currency_rates') }}
),

ranked AS (
    SELECT
        UPPER(TRIM(currency_code))          AS currency_code,
        CAST(rate_to_usd AS FLOAT)          AS rate_to_usd,
        effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY currency_code
            ORDER BY effective_date DESC
        )                                   AS rn
    FROM source
)

SELECT
    currency_code,
    rate_to_usd,
    effective_date
FROM ranked
WHERE rn = 1