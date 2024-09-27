{% macro drop_stg_tables() %}
    {% if target.name == 'prod' %}
        {% set drop_sql = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'stg%'" %}
        {% set stg_tables = run_query(drop_sql) %}

        {% for table in stg_tables %}
            {% set drop_statement = "DROP TABLE IF EXISTS " ~ table %}
            {{ log("Dropping staging table: " ~ table, info=True) }}
            {{ run_query(drop_statement) }}
        {% endfor %}
    {% else %}
        {{ log("Skipping drop of staging tables; not in production environment", info=True) }}
    {% endif %}
{% endmacro %}
