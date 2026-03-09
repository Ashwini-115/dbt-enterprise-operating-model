-- Staging for Vendors lookup table

WITH source AS (
    SELECT * FROM {{ source('raw', 'vendors') }}
)

SELECT
    vendor_id,
    TRIM(vendor_name)                       AS vendor_name,
    TRIM(vendor_type)                       AS vendor_type,
    TRIM(country)                           AS country,
    TRIM(payment_terms)                     AS payment_terms,
    is_active
FROM source
WHERE is_active = TRUE