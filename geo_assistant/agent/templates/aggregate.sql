{# templates/aggregate.sql #}
DROP TABLE IF EXISTS "{{ output_table }}";
CREATE TABLE "{{ output_table }}" AS
SELECT
{% for col in group_by %}
  "{{ col.value }}"{{ "," if not loop.last }}
{% endfor %}
  , ST_Union("{{ geometry_column }}") AS "{{ geometry_column }}"
FROM "{{ source_table }}"
GROUP BY
{% for col in group_by %}
  "{{ col.value }}"{{ "," if not loop.last }}
{% endfor %};

-- register the new geometry column
SELECT Populate_Geometry_Columns(
  'public.{{ output_table }}'::regclass
);

-- ensure pg-tileserv user can read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};
