-- Staging for Cost Centers lookup table

WITH source AS (
    SELECT * FROM {{ source('raw', 'cost_centers') }}
)

SELECT
    UPPER(TRIM(cost_center_code))           AS cost_center_code,
    TRIM(cost_center_name)                  AS cost_center_name,
    TRIM(department)                        AS department,
    TRIM(region)                            AS region,
    TRIM(manager_name)                      AS manager_name,
    is_active
FROM source
WHERE is_active = TRUE