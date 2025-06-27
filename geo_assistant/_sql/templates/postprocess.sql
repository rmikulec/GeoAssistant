{# templates/postprocess.sql.j2 #}

{% if new_base_table %}
  ALTER TABLE "{{ schema }}"."{{ table }}"
    ALTER COLUMN geometry TYPE geometry USING ST_SetSRID(geometry, {{ srid }});

  -- Add a new column for your canonical SRID and fill it
  ALTER TABLE "{{ schema }}"."{{ table }}"
    ADD COLUMN geom_{{ srid }} geometry({{ geometry_type }}, {{ srid }});

  UPDATE "{{ schema }}"."{{ table }}"
    SET geom_{{ srid }} = ST_Transform(geometry, {{ srid }});

  -- Drop the old geometry, rename the new one into place
  ALTER TABLE "{{ schema }}"."{{ table }}"
    DROP COLUMN geometry;

  ALTER TABLE "{{ schema }}"."{{ table }}"
    RENAME COLUMN geom_{{ srid }} TO geometry;
{% endif %}

-- Populate geometry_columns and other PostGIS metadata
SELECT Populate_Geometry_Columns(
  '{{ schema }}.{{ table }}'::regclass
);

-- 5) Grant read access
GRANT SELECT
  ON "{{ schema }}"."{{ table }}"
  TO PUBLIC;

-- Create the GiST index on your already-transformed geometry
CREATE INDEX IF NOT EXISTS "{{ table }}_geometry_gist_idx"
  ON "{{ schema }}"."{{ table }}"
  USING GIST (geometry);

-- Update planner statistics
ANALYZE "{{ schema }}"."{{ table }}";
