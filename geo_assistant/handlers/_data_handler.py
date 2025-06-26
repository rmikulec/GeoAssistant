import geopandas as gpd
from sqlalchemy import text

from sqlalchemy import Engine

from geo_assistant.handlers._filter import HandlerFilter
from geo_assistant.table_registry import Table
from geo_assistant._sql._sql_exec import execute_template_sql
from geo_assistant.config import Configuration


class PostGISHandler:
    """
    A handler class to interact with PostGIS tables and create spatial views for pg_tileserv.
    """
    def __init__(self):
        # tracks names of created SQL views
        # TODO: Make this a dict, where key is table schema.name and values are associated filters
        #   That way the queries can automatically use the filters to narrow down results
        self.active_tables: list[Table] = []

    def get_latlong_data(
        self,
        engine: Engine,
        lat: float,
        lon: float,
        line_tolerance: int = 10
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
        if self.active_tables:
            table = self.active_tables[0]
            results = execute_template_sql(
                template_name="lat_long",
                engine=engine,
                lat=lat,
                lon=lon,
                tolerance_meters=line_tolerance,
                schema=table.schema,
                table=table.name
                
            )

            results = [
                {k: v for k, v in result.items() if k != Configuration.geometry_column}
                for result in results
            ]

            return results
        else:
            return []


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
            sql = f"SELECT COUNT(*) FROM {table.schema}.{table.name} AS {table.name} WHERE {where_clause};"
        else:
            sql = f"SELECT COUNT(*) FROM {table.schema}.{table.name}"
        with engine.connect() as conn:
            total_count += conn.execute(text(sql)).scalar()
        return total_count