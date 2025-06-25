import geopandas as gpd
from sqlalchemy import create_engine, text
from typing import Optional, Union
from collections import defaultdict

from sqlalchemy import Engine

from geo_assistant.handlers._filter import HandlerFilter
from geo_assistant.table_registry import Table
from geo_assistant.agent._sql_exec import execute_template_sql
from geo_assistant.config import Configuration


class PostGISHandler:
    """
    A handler class to interact with PostGIS tables and create spatial views for pg_tileserv.
    """
    def __init__(self, default_table: Optional[str] = Configuration.default_table):
        self.default_table = default_table
        # tracks names of created SQL views
        self.currently_selected = []

    def get_latlong_data(
        self,
        engine: Engine,
        lat: float,
        lon: float,
        table: Table,
        line_tolerance: Optional[int] = 10
    ) -> gpd.GeoDataFrame:
        """
        Retrieves data from a table, that intersects with a given lat/long

        Args:
            engine (Engine): A sqlalchemy engine
            lat (float): The latitude value
            long (float): The longitude value
            table (str): Table to query. Defaults to value found in `geo_assistant.config`
            line_tolerance (int): The tolerance of the "buffer", in meters, for when selecting
                1-dimensional data (lines)

        Returns:
            GeoDataFrame: A DataFrame containing all rows that intersected with the given lat/long
        """
        results = execute_template_sql(
            template_name="lat_long",
            engine=engine,
            lat=lat,
            lon=lon,
            tolerance_meters=line_tolerance,
            schema=table.schema,
            table=table.name
            
        )

        return results


    def filter_count(
        self,
        engine: Engine,
        table: Table,
        filters: list[HandlerFilter] = None,
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
        if filters:
            where_clause = " AND ".join(f._to_sql() for f in filters)
            sql = f"SELECT COUNT(*) FROM {table.name} WHERE {where_clause};"
        else:
            sql = f"SELECT COUNT(*) FROM {table.name}"
        with engine.connect() as conn:
            total_count += conn.execute(text(sql)).scalar()
        return total_count