{# templates/merge.sql #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
{%- for col in select_columns %}
  "{{ col.value }}",
{%- endfor %}
  "r"."geometry"
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