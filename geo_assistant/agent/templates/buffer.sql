{# templates/buffer.sql #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
  *,
  ST_Buffer(
    "{{ geometry_column }}",
    {{ buffer_distance }} * (CASE WHEN '{{ buffer_unit }}' = 'kilometers' THEN 1000 ELSE 1 END)
  ) AS geom_buf
FROM "{{ source_table }}";

-- drop the old geometry so we only have one
ALTER TABLE "{{ output_table }}"
  DROP COLUMN IF EXISTS "{{ geometry_column }}";

-- rename the buffered column into place
ALTER TABLE "{{ output_table }}"
  RENAME COLUMN geom_buf TO "{{ geometry_column }}";

-- fix the column's typmod to MULTIPOLYGON,3857
ALTER TABLE "{{ output_table }}"
  ALTER COLUMN "{{ geometry_column }}"
    TYPE geometry(MultiPolygon,3857)
    USING "{{ geometry_column }}"::geometry(MultiPolygon,3857);

-- register in PostGIS metadata
SELECT Populate_Geometry_Columns(
  '{{ schema_name | default("public") }}.{{ output_table }}'::regclass
);

-- allow pg-tileserv to read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};
