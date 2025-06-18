{# templates/filter_step.sql.j2 #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
    *
FROM "{{ source_table }}"
{% if filters %}
WHERE
{% for f in filters %}
    {%- if f.operator in ['IN','NOT IN'] -%}
    "{{ f.column }}" {{ f.operator }}
      ({{ f.values | map('tojson') | join(', ') }})
    {%- elif f.operator == 'BETWEEN' -%}
    "{{ f.column }}" BETWEEN
      {{ f.range[0] | tojson }} AND {{ f.range[1] | tojson }}
    {%- elif f.operator in ['IS NULL','IS NOT NULL'] -%}
    "{{ f.column }}" {{ f.operator }}
    {%- else -%}
    "{{ f.column }}" {{ f.operator }} {{ f.value | tojson }}
    {%- endif -%}
    {{ ' AND' if not loop.last else '' }}
{% endfor %}
{% endif %};

-- register the new geometry column
SELECT Populate_Geometry_Columns(
  'public.{{ output_table }}'::regclass
);

-- ensure pg-tileserv user can read it
GRANT SELECT ON "{{ output_table }}" TO {{ tileserv_role | default('public') }};
