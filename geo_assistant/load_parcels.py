import geopandas as gpd
from sqlalchemy import create_engine, text

# match the names in your docker-compose:
USER = "gisuser"
DB   = "parcelsdb"
HOST = "127.0.0.1"
PORT = 5432

# 1) read your data
gdf = gpd.read_parquet("./pluto/map.parquet")

# 2) reproject to Web‐Mercator so the tile‐server can index it directly
gdf = gdf.to_crs(epsg=3857)

# 3) connect and write
engine = create_engine(f"postgresql://{USER}@{HOST}:{PORT}/{DB}")
gdf.to_postgis(
    name="parcels",        # table name
    con=engine,
    if_exists="replace",   # drop & recreate
    index=False
)
print("Loaded", len(gdf), "parcels into PostGIS.")

with engine.begin() as conn:
    # 1) enable PostGIS
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

    # 2) add PK (only if not present)
    conn.execute(text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                 WHERE conrelid = 'parcels'::regclass
                   AND contype = 'p'
            ) THEN
              ALTER TABLE parcels ADD COLUMN id SERIAL PRIMARY KEY;
            END IF;
        END$$;
    """))

with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    conn.execute(text("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS parcels_geom_gist
          ON parcels USING GIST (geometry);
    """))

with engine.begin() as conn:
    # 4) other useful indexes
    #conn.execute(text("CREATE INDEX IF NOT EXISTS parcels_borough_idx ON parcels (borough);"))

    # 5) cluster & vacuum/analyze
    conn.execute(text("CLUSTER parcels USING parcels_geom_gist;"))
    conn.execute(text("VACUUM ANALYZE parcels;"))
print("Indexes created and table analyzed.")