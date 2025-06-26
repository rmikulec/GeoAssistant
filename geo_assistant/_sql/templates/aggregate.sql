{# templates/aggregate_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS
SELECT
  {%- set aggs = aggregators | default([]) -%}
  {%- for agg in aggs %}
    {%- if agg.operator == 'COUNT' %}
      COUNT(
        {{ 'DISTINCT ' if agg.distinct else '' -}}
        {%- if agg.column.value != '*' -%}
          "{{ agg.column.value }}"
        {%- else -%}
          *
        {%- endif -%}
      )
    {%- else %}
      {{ agg.operator }}("{{ agg.column.value }}")
    {%- endif %}
    AS
      "{{ (agg.alias
           | default(agg.operator|lower ~ '_' ~ 
             (agg.column.value == '*' and 'all' or agg.column.value)
           )
         ) }}"
    {%- if not loop.last %},{% endif %}
  {%- endfor %}
  {%- if aggs|length > 0 %},{% endif %}

  -- spatial aggregation ↓
  ST_SetSRID(
    {%- if spatial_aggregator %}
      {%- set spa = spatial_aggregator | upper -%}
      {%- if spa == 'COLLECT' -%}
        ST_Collect("{{ geometry_column }}")
      {%- elif spa == 'UNION' -%}
        ST_Union("{{ geometry_column }}")
      {%- elif spa == 'CENTROID' -%}
        ST_Centroid("{{ geometry_column }}")
      {%- elif spa == 'ENVELOPE' -%}
        ST_Envelope("{{ geometry_column }}")
      {%- elif spa == 'CONVEXHULL' -%}
        ST_ConvexHull("{{ geometry_column }}")
      {%- elif spa == 'EXTENT' -%}
        -- convert the BOX2D into a polygon
        ST_ConvexHull(ST_Envelope("{{ geometry_column }}"))
      {%- else -%}
        "{{ geometry_column }}"
      {%- endif -%}
    {%- else -%}
      "{{ geometry_column }}"
    {%- endif -%}
    , {{ srid | default(4326) }}
  )::Geometry(
    {{ gtype | default('MultiLineString') }},
    {{ srid | default(4326) }}
  ) AS "{{ geometry_column }}"
FROM
  "{{ source_table.source_schema | default('public') }}"."{{ source_table.source_table | default('input') }}"
{%- if group_by %}
GROUP BY
  {%- for col in group_by %}
    "{{ col.value }}"{{ "," if not loop.last else "" }}
  {%- endfor %}
  {%- if spatial_aggregator %}, "{{ geometry_column }}"{% endif %}
{%- endif %};
