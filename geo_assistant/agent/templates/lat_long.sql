{# templates/find_by_point.sql.j2 #}
SELECT *
FROM "{{ schema }}"."{{ table }}" AS t
WHERE ST_DWithin(
  ST_Transform(t.geometry, 4326)::geography,                  -- ← transform first
  ST_SetSRID(
    ST_MakePoint({{ lon }}, {{ lat }}),
    4326
  )::geography,
  {{ tolerance_meters }}
);