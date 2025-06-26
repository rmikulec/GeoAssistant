{# templates/filter_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH src AS (
  SELECT
    {%- for col in select %}
      "{{ col.value }}"{{ "," if not loop.last else "" }}
    {%- endfor %}
    {%- if select | length > 0 %},{% endif %}
    ST_Transform(
      "{{ geometry_column }}",
      {{ srid }}
    )::Geometry({{ gtype }}, {{ srid }}) AS "{{ geometry_column }}"
  FROM "{{ source_table.source_schema }}"."{{ source_table.source_table }}"
)
SELECT *
FROM src
{%- if filters %}
WHERE
  {%- for f in filters %}
    "{{ f.column.value }}" {{ f.operator }}
    {%- if f.operator in ['IN','NOT IN'] -%}
      ({{ f.value_list | join(', ') }})
    {%- elif f.operator == 'BETWEEN' -%}
      {{ f.range[0] }} AND {{ f.range[1] }}
    {%- elif f.operator in ['IS NULL','IS NOT NULL'] -%}
      {# nothing to add #}
    {%- else -%}
      {{ f.value }}
    {%- endif -%}
    {{ "AND" if not loop.last else "" }}
  {%- endfor %}
{%- endif %};
