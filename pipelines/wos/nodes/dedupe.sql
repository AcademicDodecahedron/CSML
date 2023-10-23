{% set sep = joiner(',\n    ') -%}
CREATE TABLE {{table}}(
    id_record INTEGER PRIMARY KEY AUTOINCREMENT,
    {% for column in record_columns -%}
    {{ sep() | sql }}{{ column.definition() }}
    {%- endfor %}
);

{% set sep = joiner(',\n    ') -%}
INSERT INTO {{table}}(
    {% for column in record_columns -%}
    {{ sep() | sql }}{{ column.identifier() }}
    {%- endfor %}
)
{%- set sep = joiner(',\n    ') -%}
SELECT
    {% for column in record_columns -%}
    {{ sep() | sql }}{{ column.identifier() }}
    {%- endfor %}
FROM {{ raw }}
GROUP BY num_record_raw
HAVING MIN(ROWID);
