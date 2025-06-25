{# templates/postprocess.sql.j2 #}

-- 1) Populate the geometry columns
SELECT Populate_Geometry_Columns(
  '{{ schema }}.{{ table }}'::regclass
);

-- 2) Grant read access
GRANT SELECT
  ON "{{ schema }}"."{{ table }}"
  TO PUBLIC;

-- 3) Disable autovacuum on this truly static table
ALTER TABLE "{{ schema }}"."{{ table }}"
  SET (autovacuum_enabled = false);

-- 4) Create the GiST index without locking out queries
--    (must run outside a transaction block)
{% if not in_transaction %}
CREATE INDEX CONCURRENTLY IF NOT EXISTS
  "{{ table }}_geometry_gist_idx"
  ON "{{ schema }}"."{{ table }}"
  USING GIST (geometry);
{% else %}
-- Note: to use CONCURRENTLY you’ll need to run this statement on its own,
-- not inside a transaction. If your templating engine wraps everything
-- in BEGIN/END, drop CONCURRENTLY here or run index creation separately.
CREATE INDEX IF NOT EXISTS
  "{{ table }}_geometry_gist_idx"
  ON "{{ schema }}"."{{ table }}"
  USING GIST (geometry);
{% endif %}

-- 5) Physically cluster your rows by the new index for best I/O locality
CLUSTER "{{ schema }}"."{{ table }}"
  USING "{{ table }}_geometry_gist_idx";

-- 6) Update planner statistics one last time
ANALYZE "{{ schema }}"."{{ table }}";
