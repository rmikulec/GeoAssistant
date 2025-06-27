#!/usr/bin/env python3
import argparse
import asyncio
import sys

from geo_assistant.logging import get_logger

import geopandas as gpd
from sqlalchemy import create_engine, text

from geo_assistant.config import Configuration
from geo_assistant.utils import pick_best_geometry
from geo_assistant._sql._sql_exec import execute_template_sql
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore

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
        help="Destination table name in PostGIS (default: parcels)",
    )
    p.add_argument(
        "--metadata",
        "-m",
        type=str,
        help="The path the the metadata pdf with field definitions"
    )
    p.add_argument(
        "--dest-crs",
        type=int,
        default=Configuration.srid,
        help="EPSG code to reproject into (default: in configuration)",
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
    p.add_argument(
        "--skip-docstores",
        "-s",
        action="store_true",
        help="Skips uploading to docstores if true",
    )
    return p.parse_args()

def main():
    args = parse_args()

    gdf = gpd.read_parquet(args.parquet)

    gdf = gdf.to_crs(epsg=args.dest_crs)
    geom_types = gdf.geom_type.value_counts()
    logger.info(f"Current geom_type distribution: \n{geom_types.to_json(indent=2)}")

    if len(geom_types) > 1:
        gdf = pick_best_geometry(gdf, convert=True)
        logger.info(f"After conversion: {gdf.geom_type.value_counts()}")
    geom_type = gdf.geom_type[0]

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


    gdf.to_postgis(
        name=args.table,
        con=engine,
        schema=Configuration.db_base_schema,
        if_exists=args.if_exists,
        index=args.index
    )

    logger.info(f"Loaded {len(gdf)} features into table '{args.table}'.")

    with engine.begin() as conn:
        execute_template_sql(
            template_name="postprocess",
            engine=conn,
            schema=Configuration.db_base_schema,
            table=args.table,
            srid=Configuration.srid,
            geometry_type=geom_type,
            new_base_table=True
        )
    logger.info("Postprocessing complete")

    if not args.skip_docstores:
        logger.info(f"Loading DocStores from {args.metadata}")
        field_store = FieldDefinitionStore(version=Configuration.field_def_store_version)
        info_store = SupplementalInfoStore(version=Configuration.info_store_version)

        logger.info("loading into field store...")
        asyncio.run(field_store.add_pdf(
            pdf_path=args.metadata,
            table=args.table
        ))

        logger.info("Loading into info store...")
        asyncio.run(info_store.add_pdf(
            pdf_path=args.metadata,
            table=args.table
        ))
        

if __name__ == "__main__":
    main()
