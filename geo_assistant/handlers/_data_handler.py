import geopandas as gpd
from sqlalchemy import create_engine, text

from geo_assistant.handlers._filter import GeoFilter

class DataHandler:
    """
    A small handler class to interact directly with the PostGIS Database
    """
    
    def __init__(self, db_name: str, table_name: str):
        self.engine = create_engine(f"postgresql+psycopg2://gisuser:pw@localhost:5432/{db_name}")
        self.table_name = table_name
    
    def get_geojson(self, lat: float, long: float) -> gpd.GeoDataFrame:
        """
        Gets the geo json data based on a given lat / long

        Args:
            - lat(float); The latitude of the expected entry
            - long(float): The longitude of the expected entry
        
        Returns:
            (gpd.GeoDataFrame): A dataframe of all entries that contains the (lat/long)
        """
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


    def filter_count(self, filters: list[GeoFilter]) -> int:
        """
        Returns the count of entries in the database that adhere to the given filters

        Args:
            filters(list[GeoFilter]): The filters that the user is querying on
        Returns:
            int: The number of entries in the database that adhere to the filters
        """
        sql = f"""
        SELECT count(*)
        FROM {self.table_name}
        WHERE"""

        sql += "\n AND ".join([map_filter._to_sql() for map_filter in filters])

        with self.engine.connect() as conn:
            count = conn.execute(text(sql)).scalar()
        
        return count