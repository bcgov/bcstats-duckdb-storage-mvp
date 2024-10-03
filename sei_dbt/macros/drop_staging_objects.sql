{% macro drop_staging_objects() %}
    {% if target.name == 'prod' %}
        -- Get all objects (tables or views) that start with 'stg_'
        {% set result = run_query("SELECT table_name, table_type FROM information_schema.tables WHERE table_name LIKE 'stg%'") %}

        -- Loop over each object
        {% for row in result %}
            {% set table_name = row['table_name'] %}
            {% set table_type = row['table_type'] %}

            -- Log the object being dropped
            {{ log("Dropping " ~ table_type ~ ": " ~ table_name, info=True) }}

            -- Drop the object based on its type (Table or View)
            {% if table_type == 'VIEW' %}
                {{ run_query("DROP VIEW IF EXISTS " ~ table_name) }}
            {% else %}
                {{ run_query("DROP TABLE IF EXISTS " ~ table_name) }}
            {% endif %}

        {% endfor %}
    {% else %}
        {{ log("Not in production environment, skipping drop staging objects", info=True) }}
    {% endif %}
{% endmacro %}
