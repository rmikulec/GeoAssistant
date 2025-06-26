{# templates/find_by_point.sql.j2 #}
SELECT *
FROM "{{ schema }}"."{{ table }}" AS t
WHERE (
  -- 1-D geometries (lines): hit anything within {{ tolerance_meters }} meters
  ( ST_Dimension(t.geometry) = 1
    AND ST_DWithin(
          ST_Transform(t.geometry, 4326)::geography,
          ST_SetSRID(
            ST_MakePoint({{ lon }}, {{ lat }}),
            4326
          )::geography,
          {{ tolerance_meters }}
    )
  )
  OR
  -- 2-D geometries (polygons): exact point‐in‐polygon
  ( ST_Dimension(t.geometry) = 2
    AND ST_Contains(
          ST_Transform(t.geometry, 4326),
          ST_SetSRID(
            ST_MakePoint({{ lon }}, {{ lat }}),
            4326
          )
    )
  )
);