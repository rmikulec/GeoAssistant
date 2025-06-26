{# templates/filter_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
SELECT
{%- for col in select %}
  "{{ col.value }}",
{%- endfor %}
  ST_Transform("{{ geometry_column }}", {{ srid }})::Geometry({{ gtype }}, {{ srid }}) AS "{{ geometry_column }}"
FROM "{{ source_table.source_schema }}"."{{ source_table.source_table }}"
{% if filters %}
WHERE
{% for f in filters %}
{%- if f.operator in ['IS NULL', 'IS NOT NULL'] -%}
  "{{ f.column.value }}" {{ f.operator }}
{%- elif f.operator in ['IN', 'NOT IN'] -%}
  "{{ f.column.value }}" {{ f.operator }} ({{ f.value_list | join(', ') }})
{%- elif f.operator == 'BETWEEN' -%}
  "{{ f.column.value }}" BETWEEN {{ f.range[0] }} AND {{ f.range[1] }}
{%- else -%}
  "{{ f.column.value }}" {{ f.operator }} {{ f.value }}
{%- endif -%}
  {{ " AND " if not loop.last }}
{% endfor %}
{% endif %};
