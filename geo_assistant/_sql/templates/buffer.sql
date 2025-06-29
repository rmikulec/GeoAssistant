{# templates/buffer.sql #}
DROP TABLE IF EXISTS "{{ schema }}"."{{ output_table }}";

CREATE TABLE "{{ schema }}"."{{ output_table }}" AS

SELECT
  *,
  ST_Buffer(
    geom{{srid}},
    {{ buffer_distance }} * (CASE WHEN '{{ buffer_unit }}' = 'kilometers' THEN 1000 ELSE 1 END)
  )::Geometry(MultiPolygon, {{ srid }}) AS geom_buf
FROM "{{ source_table.source_schema }}"."{{ source_table.source_table }}";

-- drop the old geometry so we only have one
ALTER TABLE "{{ schema }}"."{{ output_table }}"
DROP COLUMN IF EXISTS "{{ geometry_column }}";

-- rename the buffered column into place
ALTER TABLE "{{ schema }}"."{{ output_table }}"
RENAME COLUMN geom_buf TO "{{ geometry_column }}";