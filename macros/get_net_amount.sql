-- MACRO: get_net_amount
-- Purpose: Calculates net GL impact based on normal balance direction
-- Used in: mart_trial_balance
-- Transformation type: Conditional logic (CASE WHEN)

{% macro get_net_amount(debit_col, credit_col, normal_balance_col) %}
    CASE
        WHEN {{ normal_balance_col }} = 'DEBIT'
            THEN {{ debit_col }} - {{ credit_col }}
        WHEN {{ normal_balance_col }} = 'CREDIT'
            THEN {{ credit_col }} - {{ debit_col }}
        ELSE
            {{ debit_col }} - {{ credit_col }}
    END
{% endmacro %}