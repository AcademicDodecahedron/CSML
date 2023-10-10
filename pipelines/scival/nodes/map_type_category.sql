{% set columns = [Column("type_category", "INT")] -%}

{% for key, value in mapping.items() %}
UPDATE {{table}}
SET type_category = {{value}}
WHERE field_name = {{key}};
{% endfor %}
