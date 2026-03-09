{% macro log_model_run() %}

    {% set log_sql %}
        INSERT INTO ANALYTICS_DEV.MARTS.dbt_run_audit (
            model_name,
            target_name,
            run_started_at,
            dbt_version,
            invocation_id
        )
        VALUES (
            '{{ this.name }}',
            '{{ target.name }}',
            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            '{{ dbt_version }}',
            '{{ invocation_id }}'
        )
    {% endset %}

    {{ return(log_sql) }}

{% endmacro %}