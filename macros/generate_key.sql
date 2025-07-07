{% macro generate_key(columns) %}
    {%- set key_parts = [] -%}
    {%- for column in columns -%}
        {%- set key_parts = key_parts.append(column) -%}
    {%- endfor -%}
    concat({{ key_parts | join(", '-', ") }})
{% endmacro %} 