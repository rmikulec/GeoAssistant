{# templates/spatial_join.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH
  l AS (
    SELECT
      {%- for col in left_select %}
        a."{{ col.value }}" AS "l_{{ col.value }}",
      {%- endfor %}
      a."geometry" AS "l_geom"
    FROM "{{ left_table.source_schema }}"."{{ left_table.source_table }}" AS a
  ),
  r AS (
    SELECT
      {%- for col in right_select %}
        b."{{ col.value }}" AS "r_{{ col.value }}",
      {%- endfor %}
      b."geometry" AS "r_geom"
    FROM "{{ right_table.source_schema }}"."{{ right_table.source_table }}" AS b
  )
SELECT
  l.*,
  r.*,
  {% set spa = (spatial_aggregator or 'CENTROID') | upper %}
  {% if spa == 'COLLECT' %}
  ST_Collect(l.l_geom, r.r_geom) AS "{{ geometry_column }}"
  {% elif spa == 'UNION' %}
  ST_Union(l.l_geom, r.r_geom) AS "{{ geometry_column }}"
  {% elif spa == 'CENTROID' %}
  ST_Centroid(ST_Collect(l.l_geom, r.r_geom)) AS "{{ geometry_column }}"
  {% elif spa == 'ENVELOPE' %}
  ST_Envelope(ST_Collect(l.l_geom, r.r_geom)) AS "{{ geometry_column }}"
  {% elif spa == 'CONVEXHULL' %}
  ST_ConvexHull(ST_Union(l.l_geom, r.r_geom)) AS "{{ geometry_column }}"
  {% elif spa == 'EXTENT' %}
  ST_ConvexHull(ST_Envelope(ST_Union(l.l_geom, r.r_geom)))
  {% endif %}
FROM l
JOIN r
  ON l.l_geom && r.r_geom
  {%- if spatial_predicate | lower == 'dwithin' %}
    AND ST_DWithin(l.l_geom, r.r_geom, {{ distance }})
  {%- else %}
    AND ST_{{ spatial_predicate | upper }}(l.l_geom, r.r_geom)
  {%- endif %};
