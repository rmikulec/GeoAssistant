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