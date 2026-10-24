{% macro generate_default_dimension_row(column_mapping) %}
    {# Generates a default -1 row for Kimball dimensional models #}
    SELECT
    {% for column_name, default_value in column_mapping.items() %}
        {{ default_value }} AS {{ column_name }}
        {%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
{% endmacro %}