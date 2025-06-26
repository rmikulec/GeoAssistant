{# templates/overlay.sql #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
SELECT
  ST_{{ spatial_predicate }}(r.geom, m.geom) AS intersection_geom,
  ST_Length(r.geom) AS rd_orig_length,
  r.*
FROM {{ right_table }} AS r
JOIN {{ left_table }} AS l
  ON ST_{{ spatial_predicate }}(r.geom, l.geom)
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

