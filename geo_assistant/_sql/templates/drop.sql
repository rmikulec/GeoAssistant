{# templates/drop.sql #}

-- 1. drop the spatial index
DROP INDEX IF EXISTS {{ table_name }}_geometry_idx;

-- 2. unregister the geometry column
-- (this removes the entry from geometry_columns)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_name }}'
      AND table_name   = '{{ table_name }}'
      AND column_name  = 'geometry'
  ) THEN
    PERFORM DropGeometryColumn(
      '{{ schema_name }}',
      '{{ table_name }}',
      'geometry'
    );
  END IF;
END $$;

-- 3. revoke read access from the tileserv role
REVOKE SELECT ON "{{ schema_name }}"."{{ table_name }}" FROM PUBLIC;

-- 4. drop the table and all dependents
DROP TABLE IF EXISTS "{{ schema_name }}"."{{ table_name }}" CASCADE;

