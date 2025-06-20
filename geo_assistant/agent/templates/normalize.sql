-- Normalize geometry column to uniform type & SRID
ALTER TABLE {{ table }}
  ALTER COLUMN {{ geometry_column }}
  TYPE Geometry({{ typmod }},{{ srid }})
  USING
    (ST_Multi(
       ST_Transform({{ geometry_column }},{{ srid }})
     ))::Geometry({{ typmod }},{{ srid }});

-- Re-register for pg-tileserv
SELECT Populate_Geometry_Columns({{ table }}::regclass);