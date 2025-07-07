{% macro get_strtype_schema() %}
    {% if target.name == 'dev' %}
        {{ return('string') }}
    {% elif target.name == 'dev2' %}
        {{ return('NVARCHAR') }}
    {% else %}
        {{ return('NVARCHAR') }}
    {% endif %}
{% endmacro %}