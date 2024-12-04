{% macro drop_staging_objects() %}
    {% if target.name == 'prod' %}
        {% set relations = dbt_utils.get_relations_by_prefix(target.schema, 'stg_') %}
        {% for relation in relations %}
            {% set relation_name = relation.identifier %}
            {{ log('Dropping relation: ' ~ relation_name, info=True) }}
            
            -- Attempt to drop as a view
            {{ run_query("DROP VIEW IF EXISTS " ~ relation_name) }}
            
            -- Attempt to drop as a table
            {{ run_query("DROP TABLE IF EXISTS " ~ relation_name) }}
            
        {% endfor %}
    {% else %}
        {{ log("Not in production environment, skipping drop of staging objects", info=True) }}
    {% endif %}
{% endmacro %}
