#!/usr/bin/env python3
import argparse
import sys

from geo_assistant.logging import get_logger

import geopandas as gpd
from sqlalchemy import create_engine, text

from geo_assistant.config import Configuration
from geo_assistant.agent._sql_exec import execute_template_sql

logger = get_logger(__name__)

def parse_args():
    p = argparse.ArgumentParser(
        description="Load a GeoParquet into PostGIS (reprojecting to Web-Mercator by default)."
    )
    p.add_argument(
        "--parquet",
        "-p",
        help="Path to the input Parquet file (e.g. ./pluto/map.parquet)",
    )
    p.add_argument(
        "--table",
        "-t",
        default="parcels",
        help="Destination table name in PostGIS (default: parcels)",
    )
    p.add_argument(
        "--src-crs",
        type=int,
        help="EPSG code of source CRS (if not set, uses whatever the Parquet declares)",
    )
    p.add_argument(
        "--metadata",
        type=str,
        help="The path the the metadata pdf with field definitions"
    )
    p.add_argument(
        "--dest-crs",
        type=int,
        default=3857,
        help="EPSG code to reproject into (default: 3857 = Web-Mercator)",
    )
    p.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="Behavior if the table already exists (default: replace)",
    )
    p.add_argument(
        "--index",
        "-i",
        action="store_true",
        help="Write the DataFrame index as a column in the table",
    )
    return p.parse_args()

def main():
    args = parse_args()

    # 1) load
    try:
        gdf = gpd.read_parquet(args.parquet)
    except Exception as e:
        logger.error(f"Error reading Parquet: {e}")
        sys.exit(1)

    # 2) enforce source CRS if provided
    if args.src_crs:
        gdf = gdf.set_crs(epsg=args.src_crs, allow_override=True)

    # 3) reproject
    gdf = gdf.to_crs(epsg=args.dest_crs)

    # 4) connect
    engine = create_engine(Configuration.db_connection_url)

    # Create the 'base' schema
    with engine.begin() as conn:
        sql = text(
            (
                f"CREATE SCHEMA IF NOT EXISTS {Configuration.db_base_schema} AUTHORIZATION pg_database_owner;"
                f"GRANT USAGE ON SCHEMA {Configuration.db_base_schema} TO pg_database_owner;"
            )
        )
        conn.execute(sql)

    # 5) write to PostGIS
    try:
        gdf.to_postgis(
            name=args.table,
            con=engine,
            schema='base',
            if_exists=args.if_exists,
            index=args.index
        )
    except Exception as e:
        logger.error(f"Error writing to PostGIS: {e}")
        sys.exit(1)

    logger.info(f"Loaded {len(gdf)} features into table '{args.table}'.")

    # 6) create spatial index on the geometry column
    geom_col = gdf.geometry.name  # usually "geometry"
    
    try:
        with engine.raw_connection() as raw_conn:
        # Turn off SQLAlchemy’s transaction management completely
            raw_conn.autocommit = True
            cursor = raw_conn.cursor()
            execute_template_sql(
                template_name="postprocess",
                engine=cursor,
                schema=Configuration.db_base_schema,
                table=args.table
            )
    except Exception as e:
        logger.error(f"Error setting up table: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
