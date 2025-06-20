{# templates/postprocess.sql.j2 #}
-- Post-process for pg_tileserv compliance using provided geometry type & SRID

ALTER TABLE "{{ schema_name }}"."{{ output_table }}"
  ALTER COLUMN "{{ geometry_column }}"
  TYPE Geometry({{ gtype }}, {{ srid }})
  USING ST_SetSRID("{{ geometry_column }}", {{ srid }})::Geometry({{ gtype }}, {{ srid }});

-- register in geometry_columns for pg_tileserv
SELECT Populate_Geometry_Columns(
  '{{ schema_name }}.{{ output_table }}'::regclass
);

-- grant read access
GRANT SELECT ON "{{ schema_name }}"."{{ output_table }}" TO "{{ tileserv_role }}";
