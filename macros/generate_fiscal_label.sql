-- MACRO: generate_fiscal_label
-- Purpose: Generates a readable fiscal period label like FY2024-P01
-- Used in: mart_trial_balance, mart_vendor_spend

{% macro generate_fiscal_label(fiscal_year_col, fiscal_period_col) %}
    CONCAT(
        'FY', CAST({{ fiscal_year_col }} AS VARCHAR),
        '-P', LPAD(CAST({{ fiscal_period_col }} AS VARCHAR), 2, '0')
    )
{% endmacro %}