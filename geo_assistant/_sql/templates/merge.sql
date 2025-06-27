{# templates/spatial_join.sql.j2 #}
-- Leaving in the CTEs for now, as may want to add where clauses

DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH
  l AS (
    SELECT
      {%- for col in left_select %}
        a."{{ col.value }}" AS "l_{{ col.value }}",
      {%- endfor %}
    FROM "{{ left_table.source_schema }}"."{{ left_table.source_table }}" AS a
  ),
  r AS (
    SELECT
      {%- for col in right_select %}
        b."{{ col.value }}" AS "r_{{ col.value }}",
      {%- endfor %}
    FROM "{{ right_table.source_schema }}"."{{ right_table.source_table }}" AS b
  )
SELECT
  l.*,
  r.*
FROM l
JOIN r
  ON l.geom_l && r.geom_r
  {%- if spatial_predicate | lower == 'dwithin' %}
    AND ST_DWithin(l.geom_l, r.geom_r, {{ distance }})
  {%- else %}
    AND ST_{{ spatial_predicate | upper }}(l.geom_l, r.geom_r)
  {%- endif %};
