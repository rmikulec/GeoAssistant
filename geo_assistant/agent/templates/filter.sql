{# templates/filter_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
    *
FROM "{{ source_table }}"
{% if filters %}
WHERE
{% for f in filters %}
    {%- if f.operator in ['IS NULL', 'IS NOT NULL'] -%}
    "{{ f.column.value }}" {{ f.operator }}
    {%- elif f.operator in ['IN', 'NOT IN'] -%}
    "{{ f.column.value }}" {{ f.operator }} ({{ f.values | join(', ') }})
    {%- elif f.operator == 'BETWEEN' -%}
    "{{ f.column.value }}" BETWEEN {{ f.range[0] }} AND {{ f.range[1] }}
    {%- else -%}
    "{{ f.column.value }}" {{ f.operator }} {{ f.value }}
    {%- endif -%}
    {{ " AND " if not loop.last }}
{% endfor %}
{% endif %};


-- === Geometry normalization ===
ALTER TABLE public.{{ output_table }}
  ALTER COLUMN {{ geometry_column }}
  TYPE Geometry({{ target_geometry_type }}, {{ target_srid }})
  USING
    ST_Multi(
      ST_Transform({{ geometry_column }}, {{ target_srid }})
    );

-- re-register now that type & SRID are fixed
SELECT Populate_Geometry_Columns(
  'public.{{ output_table }}'::regclass
);

-- ensure pg-tileserv user can read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};

-- now add a spatial index for fast spatial queries
CREATE INDEX ON "{{ output_table }}" USING GIST (geometry);
ANALYZE "{{ output_table }}"
