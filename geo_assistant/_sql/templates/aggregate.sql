{# templates/aggregate_step.sql.j2 #}
{% set agg_cols = select
    | map(attribute='column.value')
    | reject('equalto', '*')
    | unique
    | list
%}

DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

-- Leaving in the CTEs for now as may want to add where clauses

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
WITH src AS (
  SELECT
    {% for col in group_by %}
      "{{ col.value }}"{% if not loop.last or select|length>0%},{% endif %}
    {% endfor %}
    {% for agg in agg_cols if agg != '*' %}
      "{{ agg }}",
    {% endfor %}
      "{{ geometry_column }}"
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
      {{ agg.operator }}(src."{{ agg.column.value }}") AS "{{ agg.alias | default(agg.operator | lower ~ '_' ~ agg.column.value) }}",
    {% endif %}
  {% endfor %}
  {# Geometry Aggregation #}
  {% set spa = (spatial_aggregator or 'CENTROID') | upper %}
  {% if spa == 'COLLECT' %}
    ST_Collect("{{ geometry_column }}") AS "{{ geometry_column }}"
  {% elif spa == 'UNION' %}
    ST_Union("{{ geometry_column }}") AS "{{ geometry_column }}"
  {% elif spa == 'CENTROID' %}
    ST_Centroid(ST_Collect("{{ geometry_column }}")) AS "{{ geometry_column }}"
  {% elif spa == 'ENVELOPE' %}
    ST_Envelope(ST_Collect("{{ geometry_column }}")) AS "{{ geometry_column }}"
  {% elif spa == 'CONVEXHULL' %}
    ST_ConvexHull(ST_Union("{{ geometry_column }}", .99)) AS "{{ geometry_column }}"
  {% elif spa == 'CONCAVEHULL' %}
    ST_ConvexHull(ST_Union("{{ geometry_column }}", .50)) AS "{{ geometry_column }}"
  {% elif spa == 'EXTENT' %}
    ST_ConvexHull(ST_Envelope(ST_Union("{{ geometry_column }}"))) AS "{{ geometry_column }}"
  {% endif %}

FROM src
GROUP BY
  {% for col in group_by %}
    "{{ col.value }}"{% if not loop.last %},{% endif %}
  {% endfor %};