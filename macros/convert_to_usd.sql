-- MACRO: convert_to_usd
-- Purpose: Converts any currency amount to USD using the rates lookup
-- Used in: mart_gl_detail, mart_vendor_spend
-- Transformation type: Lookup enrichment + calculation

{% macro convert_to_usd(amount_col, rate_col) %}
    ROUND({{ amount_col }} * {{ rate_col }}, 2)
{% endmacro %}