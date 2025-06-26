{# templates/spatial_join.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH
  a AS (
    SELECT
      {% if right_select|length == 0 %}
      l.*,
      {% else %}
      {%- for col in left_select %}
        l."{{ col.value }}"{{ "," if not loop.last or right_select|length > 0 else "" }}
      {%- endfor %}
      {% endif %}
      ST_Transform(
        l."{{ geometry_column }}",
        {{ srid }}
      ) AS geom_a
    FROM "{{ left_table.source_schema }}"."{{ left_table.source_table }}" AS l
  ),
  b AS (
    SELECT
      {% if left_select|length == 0 %}
      l.*,
      {% else %}
      {%- for col in right_select %}
        l."{{ col.value }}"{{ "," if not loop.last or left_select|length > 0 else "" }}
      {%- endfor %}
      {% endif %}
      ST_Transform(
        r."{{ geometry_column }}",
        {{ srid }}
      ) AS geom_b
    FROM "{{ right_table.source_schema }}"."{{ right_table.source_table }}" AS r
  )
SELECT
  a.*,
  b.*,
  a.geom_a   AS "{{ geometry_column }}"
FROM a
JOIN b
  ON a.geom_a && b.geom_b,                        -- fast bbox pre-filter
  {%- if spatial_predicate | lower == 'dwithin' %}
    AND ST_DWithin(a.geom_a, b.geom_b, {{ distance }})
  {%- else %}
    AND ST_{{ spatial_predicate | upper }}(a.geom_a, b.geom_b)
  {%- endif %};
