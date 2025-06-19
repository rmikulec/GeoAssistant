{# templates/drop.sql #}
{% for output_table in output_tables %}
-- 1. drop the spatial index
DROP INDEX IF EXISTS "{{ output_table }}_geometry_idx";

-- 2. unregister the geometry column
-- (this removes the entry from geometry_columns)
SELECT DropGeometryColumn(
  'public',             -- schema
  '{{ output_table }}', -- table
  'geometry'            -- column
);

-- 3. revoke read access from the tileserv role
REVOKE SELECT ON "{{ output_table }}" FROM {{ tileserv_role | default('public') }};

-- 4. drop the table and all dependents
DROP TABLE IF EXISTS "{{ output_table }}" CASCADE;

{% endfor %}