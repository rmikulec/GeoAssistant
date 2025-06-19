{# templates/merge.sql #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
  l.*
FROM "{{ left_table }}" AS l
JOIN "{{ right_table }}" AS r
  ON
    {%- if spatial_predicate | lower == 'dwithin' %}
      ST_DWithin(
        l."{{ geometry_column }}",
        r."{{ geometry_column }}",
        {{ distance }}
      )
    {%- else %}
      ST_{{ spatial_predicate | upper }}(
        l."{{ geometry_column }}",
        r."{{ geometry_column }}"
      )
    {%- endif %};


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

-- ensure pg-tileserv can see it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};

-- now add a spatial index for fast spatial queries
CREATE INDEX ON "{{ output_table }}" USING GIST (geometry);
ANALYZE "{{ output_table }}"