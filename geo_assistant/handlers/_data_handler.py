import geopandas as gpd
from sqlalchemy import create_engine, text
from typing import Optional, Union
from collections import defaultdict

from sqlalchemy import Engine

from geo_assistant.handlers._filter import HandlerFilter
from geo_assistant.config import Configuration


class PostGISHandler:
    """
    A handler class to interact with PostGIS tables and create spatial views for pg_tileserv.
    """
    def __init__(self, default_table: Optional[str] = Configuration.default_table):
        self.default_table = default_table
        # tracks names of created SQL views
        self.created_views: list[str] = []

    def get_latlong_data(
        self,
        engine: Engine,
        lat: float,
        long: float,
        table: Optional[str] = Configuration.default_table,
        geometry_column: Optional[str] = Configuration.geometry_column
    ) -> gpd.GeoDataFrame:
        """
        Retrieves data from a table, that intersects with a given lat/long

        Args:
            engine (Engine): A sqlalchemy engine
            lat (float): The latitude value
            long (float): The longitude value
            table (str): Table to query. Defaults to value found in `geo_assistant.config`
            geometry_column (str): The column holding the table's geometry.
                Defaults to value found in `geo_assistant.config`

        Returns:
            GeoDataFrame: A DataFrame containing all rows that intersected with the given lat/long
        """
        sql = f"""
        SELECT *
        FROM {table}
        WHERE ST_Intersects(
            ST_Transform({geometry_column}, 4326),
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
        );
        """

        return gpd.read_postgis(
            sql,
            con=engine,
            geom_col=geometry_column,
            crs="EPSG:4326",
            params={"lat": lat, "lon": long}
        )

    def filter_count(
        self,
        engine: Engine,
        table: str,
        filters: list[HandlerFilter],
    ) -> int:
        """
        Counts the number of rows found with a given set of filters. Useful for returning data
            to the LLM for a user-friendly response

        Args:
            engine (Engine): A sqlalchemy engine
            filters (list[HandlerFilter]): Handler filters to be used for the query.
        
        Returns:
            int: The number of rows that meet the criterial of the filter
        """
        total_count = 0
        where_clause = " AND ".join(f._to_sql() for f in filters)
        sql = f"SELECT COUNT(*) FROM {table} WHERE {where_clause};"

        with engine.connect() as conn:
            total_count += conn.execute(text(sql)).scalar()
        return total_count