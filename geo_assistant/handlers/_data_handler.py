import geopandas as gpd
from sqlalchemy import create_engine, text
from typing import Optional, List, Union
from collections import defaultdict

from geo_assistant.handlers._filter import GeoFilter
from geo_assistant.config import Configuration


class DataHandler:
    """
    A handler class to interact with PostGIS tables and create spatial views for pg_tileserv.
    """
    def __init__(self, default_table: Optional[str] = None):
        self.engine = create_engine(Configuration.db_connection_url)
        self.default_table = default_table
        # tracks names of created SQL views
        self.created_views: List[str] = []

    def get_geojson(
        self,
        lat: float,
        long: float,
        table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        tbl = table or self.default_table
        if not tbl:
            raise ValueError("Must supply a table (or set default_table on init).")

        sql = f"""
        SELECT *
        FROM {tbl}
        WHERE ST_Intersects(
            ST_Transform(geometry, 4326),
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
        );
        """

        return gpd.read_postgis(
            sql,
            con=self.engine,
            geom_col="geometry",
            crs="EPSG:4326",
            params={"lat": lat, "lon": long}
        )

    def filter_count(
        self,
        filters: List[GeoFilter],
    ) -> int:
        sorted_filters = defaultdict(list)
        for filter_ in filters:
            sorted_filters[filter_.table].append(filter_)

        total_count = 0
        for table, filters_ in sorted_filters.items():
            where_clause = " AND ".join(f._to_sql() for f in filters)
            sql = f"SELECT COUNT(*) FROM {table} WHERE {where_clause};"

            with self.engine.connect() as conn:
                total_count += conn.execute(text(sql)).scalar()
        return total_count

    def spatial_join(
        self,
        source_table: str,
        target_table: str,
        distance_meters: Optional[Union[int, float]] = None,
        geodetic: bool = True
    ) -> gpd.GeoDataFrame:
        if distance_meters is None:
            join_cond = "ST_Intersects(s.geometry, t.geometry)"
        else:
            if geodetic:
                join_cond = (
                    "ST_DWithin("
                    "ST_Transform(s.geometry, 4326)::geography, "
                    "ST_Transform(t.geometry, 4326)::geography, "
                    f"{distance_meters}"  # meters
                    ")"
                )
            else:
                join_cond = f"ST_DWithin(s.geometry, t.geometry, {distance_meters})"

        sql = f"""
        SELECT DISTINCT s.*
        FROM {source_table} AS s
        JOIN {target_table} AS t
          ON {join_cond};
        """

        return gpd.read_postgis(
            sql,
            con=self.engine,
            geom_col="geometry",
            crs="EPSG:4326" if geodetic else None
        )

    def create_spatial_view(
        self,
        view_name: str,
        source_table: str,
        target_table: str,
        distance_meters: Optional[Union[int, float]] = None,
        geodetic: bool = True,
        replace: bool = False
    ) -> str:
        """
        Create (or replace) a SQL view for the spatial join of two tables.
        Registers the view in `created_views` so pg_tileserv can serve it.
        """
        # build join condition
        if distance_meters is None:
            join_cond = "ST_Intersects(s.geometry, t.geometry)"
        else:
            if geodetic:
                join_cond = (
                    "ST_DWithin("
                    "ST_Transform(s.geometry, 4326)::geography, "
                    "ST_Transform(t.geometry, 4326)::geography, "
                    f"{distance_meters}"  # meters
                    ")"
                )
            else:
                join_cond = f"ST_DWithin(s.geometry, t.geometry, {distance_meters})"

        # assemble CREATE VIEW statement
        prefix = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW"
        view_sql = (
            f"{prefix} {view_name} AS "
            f"SELECT DISTINCT s.* FROM {source_table} s "
            f"JOIN {target_table} t ON {join_cond};"
        )

        with self.engine.connect() as conn:
            conn.execute(text(view_sql))

        if view_name not in self.created_views:
            self.created_views.append(view_name)
        return view_name

    def list_views(self) -> List[str]:
        """
        Return the list of spatial views created by this handler.
        """
        return self.created_views

    def run_sql(
        self,
        sql: str,
        params: Optional[dict] = None
    ) -> gpd.GeoDataFrame:
        """
        A generic helper to run any SQL and return a GeoDataFrame.
        """
        return gpd.read_postgis(sql, con=self.engine, geom_col="geometry", crs="EPSG:4326", params=params)
