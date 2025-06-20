{# templates/aggregate.sql.j2 #}
-- drop any existing
DROP TABLE IF EXISTS "{{ output_table }}";

-- create with proper geometry typmod & SRID baked in
CREATE TABLE "{{ output_table }}" AS
SELECT
{% for agg in aggregators -%}
  {%- if agg.operator == 'COUNT' -%}
    COUNT({{ "DISTINCT " if agg.distinct else "" }}
          {{ '"' ~ agg.column.value ~ '"' if agg.column.value != '*' else '*' }})
  {%- else -%}
    {{ agg.operator }}("{{ agg.column.value }}")
  {%- endif -%}
  AS "{{ agg.alias or (agg.operator|lower ~ '_' ~ (agg.column.value|replace('*','all'))) }}",
{%- endfor %}
  {# spatial aggregation with cast #}
  ST_SetSRID(
    {%- if spatial_aggregator -%}
      ST_{{ spatial_aggregator }}("{{ geometry_column }}")
    {%- else -%}
      ST_Union("{{ geometry_column }}")
    {%- endif -%},
    {{ srid }}
  )::Geometry({{ gtype }}, {{ srid }})
  AS "{{ geometry_column }}"
FROM "{{ source_table }}"
{% if group_by %}
GROUP BY
  {%- for col in group_by -%}
    "{{ col.value }}"{{ "," if not loop.last }}
  {%- endfor %}
{% endif %}
;
