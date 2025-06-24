{# templates/merge.sql #}
DROP TABLE IF EXISTS "{{ schema }}.{{ output_table }}";

CREATE TABLE "{{ schema }}.{{ output_table }}" AS
SELECT
{%- for col in left_select %}
  l."{{ col.value }}",
{%- endfor %}
{%- for col in right_select %}
  r."{{ col.value }}",
{%- endfor %}
  ST_SetSRID(
    ST_Transform(l."{{ geometry_column }}", {{ srid }}),
    {{ srid }}
  )::Geometry({{ output_geometry_type }}, {{ srid }}) AS "{{ geometry_column }}"
FROM "{{ left_table }}" AS l
JOIN "{{ right_table }}" AS r
ON
{%- if spatial_predicate | lower == 'dwithin' %}
  ST_DWithin(
    l."{{ geometry_column }}",
    r."{{ geometry_column }}",
    {{ distance }}
  )
  {%- else %}
  ST_{{ spatial_predicate | upper }}(
    l."{{ geometry_column }}",
    r."{{ geometry_column }}"
  )
{%- endif %};