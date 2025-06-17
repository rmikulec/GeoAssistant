import geopandas as gpd
from sqlalchemy import create_engine, text

from geo_assistant.handlers._filter import GeoFilter

class DataHandler:
    
    def __init__(self, db_name: str, table_name: str):
        self.engine = create_engine(f"postgresql+psycopg2://gisuser:pw@localhost:5432/{db_name}")
        self.table_name = table_name
    
    def get_geojson(self, lat: float, long: float):
        sql = f"""
        SELECT *
        FROM {self.table_name}
        WHERE ST_Intersects(
            ST_Transform(geometry, 4326),
            ST_SetSRID(ST_MakePoint({long}, {lat}), 4326)
        );
        """

        gdf_match = gpd.read_postgis(
            sql,
            con=self.engine,
            geom_col="geometry",
            crs="EPSG:4326",
        )

        return gdf_match


    def filter_count(self, filters: list[GeoFilter]):
        sql = f"""
        SELECT count(*)
        FROM {self.table_name}
        WHERE"""

        sql += "\n AND".join([map_filter._to_sql() for map_filter in filters])

        with self.engine.connect() as conn:
            count = conn.execute(text(sql)).scalar()
        
        return count