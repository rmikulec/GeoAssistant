{# templates/merge.sql #}
{% macro quote_ident(id) -%}
  "{{ id | replace('"','""') }}"
{%- endmacro %}

DROP TABLE IF EXISTS {{ quote_ident(output_table) }};

CREATE TABLE {{ quote_ident(output_table) }} AS
WITH spatial_join AS (
  SELECT
    {%- if select %}
      {#-- join the list of columns, each safely quoted --#}
      {{ select
         | map(attribute='value')
         | map('replace', '"', '""')
         | map('quote_ident')
         | join(',\n    ')
      }},
    {%- endif %}
    {#-- always include the right-table geometry as “geometry” --#}
    r.{{ quote_ident(geometry_column) }} AS geometry
  FROM {{ quote_ident(left_table) }} AS l
  JOIN {{ quote_ident(right_table) }} AS r
    ON
    {%- if spatial_predicate|lower == 'dwithin' %}
      ST_DWithin(
        l.{{ quote_ident(geometry_column) }},
        r.{{ quote_ident(geometry_column) }},
        {{ distance }}
      )
    {%- else %}
      ST_{{ spatial_predicate|upper }}(
        l.{{ quote_ident(geometry_column) }},
        r.{{ quote_ident(geometry_column) }}
      )
    {%- endif %}
)
SELECT * 
FROM spatial_join;
