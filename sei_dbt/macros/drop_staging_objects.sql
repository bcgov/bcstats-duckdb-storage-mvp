{% macro drop_staging_objects() %}
    {% if target.name == 'prod' %}
        {% set relations = dbt_utils.get_relations_by_prefix(target.schema, 'stg_') %}
        {% for relation in relations %}
            {% set relation_name = relation.identifier %}
            {% set relation_type = relation.relation_type %}  -- Capture relation type
            
            {{ log('Dropping relation: ' ~ relation_name ~ ' of type: ' ~ relation_type, info=True) }}
            
            -- Conditionally drop based on the type
            {% if relation_type == 'view' %}
                {{ run_query("DROP VIEW IF EXISTS " ~ relation_name) }}
            {% elif relation_type == 'table' %}
                {{ run_query("DROP TABLE IF EXISTS " ~ relation_name) }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("Not in production environment, skipping drop of staging objects", info=True) }}
    {% endif %}
{% endmacro %}
