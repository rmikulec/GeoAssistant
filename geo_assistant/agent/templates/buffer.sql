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

-- allow pg-tileserv to read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};


-- now add a spatial index for fast spatial queries
CREATE INDEX ON "{{ output_table }}" USING GIST (geometry);
ANALYZE "{{ output_table }}"