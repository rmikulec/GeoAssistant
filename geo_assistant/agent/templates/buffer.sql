{# templates/buffer.sql #}
DROP TABLE IF EXISTS "{{ output_table }}";

CREATE TABLE "{{ output_table }}" AS
SELECT
  {%- for col in select %}
  "{{ col.value }}"{{ "," if not loop.last }}
  {%- endfor %}
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