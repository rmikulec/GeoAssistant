{# templates/postprocess.sql #}

-- 1) Populate the geometry columns
SELECT Populate_Geometry_Columns(
  '{{ schema }}.{{ table }}'::regclass
);

-- 2) Grant read access
GRANT SELECT
  ON "{{ schema }}"."{{ table }}"
  TO PUBLIC;

-- 3) Create the GiST index (unqualified index name so we don’t re-qualify it by schema)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
  "{{ table }}_geometry_gist_idx"
  ON "{{ schema }}"."{{ table }}"
  USING GIST (geometry);

-- 4) Update planner statistics
ANALYZE "{{ schema }}"."{{ table }}";
