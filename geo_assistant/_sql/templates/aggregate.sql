{# templates/aggregate_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH src AS (
  SELECT
    {% for col in group_by %}
      "{{ col.value }}",
    {% endfor %}
    {% for agg in select if agg.column.value != '*' %}
      "{{ agg.column.value }}",
    {% endfor %}
    ST_Transform("{{ geometry_column }}", {{ srid }})::Geometry({{ gtype }}, {{ srid }}) AS "geom{{ srid }}"
  FROM "{{ source_table.source_schema }}"."{{ source_table.source_table }}"
)

SELECT
  {% for col in group_by %}
    src."{{ col.value }}",
  {% endfor %}
  {% for agg in select %}
    {% if agg.operator == 'COUNT' %}
      COUNT({{ 'DISTINCT ' if agg.distinct else '' }}{{ '"{}"'.format(agg.column.value) if agg.column.value != '*' else '*' }}) AS "{{ agg.alias | default('count_' ~ agg.column.value) }}",
    {% else %}
      {{ agg.operator }}("{{ agg.column.value }}") AS "{{ agg.alias | default(agg.operator | lower ~ '_' ~ agg.column.value) }}",
    {% endif %}
  {% endfor %}
  {# Geometry Aggregation #}
  {% set spa = (spatial_aggregator or 'CENTROID') | upper %}
  {% if spa == 'COLLECT' %}
    ST_Collect("geom{{ srid }}") AS "geom{{ srid }}"
  {% elif spa == 'UNION' %}
    ST_Union("geom{{ srid }}") AS "geom{{ srid }}"
  {% elif spa == 'CENTROID' %}
    ST_Centroid(ST_Collect("geom{{ srid }}")) AS "geom{{ srid }}"
  {% elif spa == 'ENVELOPE' %}
    ST_Envelope(ST_Collect("geom{{ srid }}")) AS "geom{{ srid }}"
  {% elif spa == 'CONVEXHULL' %}
    ST_ConvexHull(ST_Union("geom{{ srid }}")) AS "geom{{ srid }}"
  {% elif spa == 'EXTENT' %}
    ST_ConvexHull(ST_Envelope(ST_Union("geom{{ srid }}"))) AS "geom{{ srid }}"
  {% endif %}

FROM src
GROUP BY
  {% for col in group_by %}
    "{{ col.value }}"{% if not loop.last %},{% endif %}
  {% endfor %};

-- Drop the original geometry column
ALTER TABLE "{{ schema }}"."{{ output_table }}"
DROP COLUMN IF EXISTS "{{ geometry_column }}";

-- Rename the aggregated geometry column
ALTER TABLE "{{ schema }}"."{{ output_table }}"
RENAME COLUMN "geom{{ srid }}" TO "{{ geometry_column }}";
