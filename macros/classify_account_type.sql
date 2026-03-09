-- MACRO: classify_account_type
-- Purpose: Classifies accounts into financial statement categories
-- Used in: mart_gl_detail, mart_trial_balance
-- Transformation type: CASE WHEN + Lookup enrichment

{% macro classify_account_type(account_type_col) %}
    CASE
        WHEN {{ account_type_col }} IN ('Asset', 'Liability', 'Equity')
            THEN 'Balance Sheet'
        WHEN {{ account_type_col }} IN ('Revenue', 'Expense')
            THEN 'Profit & Loss'
        ELSE
            'Unclassified'
    END
{% endmacro %}


-- MACRO: classify_account_subtype
-- Purpose: Further classifies account into more granular financial categories

{% macro classify_account_subtype(account_type_col, normal_balance_col, net_amount_col) %}
    CASE
        WHEN {{ account_type_col }} = 'Revenue'
            THEN 'Income'
        WHEN {{ account_type_col }} = 'Expense'
            THEN 'Cost'
        WHEN {{ account_type_col }} = 'Asset'
             AND {{ normal_balance_col }} = 'DEBIT'
            THEN 'Operating Asset'
        WHEN {{ account_type_col }} = 'Liability'
             AND {{ normal_balance_col }} = 'CREDIT'
            THEN 'Operating Liability'
        WHEN {{ account_type_col }} = 'Equity'
            THEN 'Shareholders Equity'
        ELSE
            'Other'
    END
{% endmacro %}