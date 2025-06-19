{# templates/aggregate.sql.j2 #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
{% if group_by %}
  {%- for col in group_by %}
  "{{ col.value }}"{{ "," }}
  {%- endfor %}
{% endif %}
{# -- regular aggregators -- #}
{% for agg in aggregators %}
  {%- if agg.operator == 'COUNT' -%}
  COUNT({{ "DISTINCT " if agg.distinct else "" }}{{ '"' ~ agg.column.value ~ '"' if agg.column.value != '*' else '*' }})
  {%- else -%}
  {{ agg.operator }}("{{ agg.column.value }}")
  {%- endif -%}
  AS "{{ agg.alias or (agg.operator|lower ~ '_' ~ (agg.column.value|replace('*','all'))) }}"{{ "," }}
{%- endfor %}
{# -- spatial aggregators -- #}
{% if spatial_aggregator %}
  ST_{{ spatial_aggregator }}("{{ geometry_column }}")
  AS "{{ geometry_column }}"
{% else %}
  ST_Union("{{ geometry_column }}") AS "{{ geometry_column }}"
{% endif %}
FROM "{{ source_table }}"
{% if group_by %}
GROUP BY
  "geometry",
  {%- for col in group_by %}
  "{{ col.value }}"{{ "," if not loop.last }}
  {%- endfor %}
{% endif %};

-- register the new geometry column
SELECT Populate_Geometry_Columns(
  'public.{{ output_table }}'::regclass
);

-- ensure pg-tileserv user can read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};

-- now add a spatial index for fast spatial queries
CREATE INDEX ON "{{ output_table }}" USING GIST ("{{ geometry_column }}");
