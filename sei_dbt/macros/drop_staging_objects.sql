{% macro drop_staging_objects() %}
    {% if target.name == 'prod' %}
        {% set relations = dbt_utils.get_relations_by_prefix(target.schema, 'stg_') %}
        {% for relation in relations %}
            {% set relation_name = relation.identifier %}
            {{ log('Dropping relation: ' ~ relation_name, info=True) }}

            -- Attempt to drop as a view
            {% set drop_view_sql = "DROP VIEW IF EXISTS " ~ relation_name %}
            {{ log('Running SQL: ' ~ drop_view_sql, info=True) }}
            {{ run_query(drop_view_sql) }}

            -- Attempt to drop as a table
            {% set drop_table_sql = "DROP TABLE IF EXISTS " ~ relation_name %}
            {{ log('Running SQL: ' ~ drop_table_sql, info=True) }}
            {{ run_query(drop_table_sql) }}

        {% endfor %}
    {% else %}
        {{ log("Not in production environment, skipping drop of staging objects", info=True) }}
    {% endif %}
{% endmacro %}
