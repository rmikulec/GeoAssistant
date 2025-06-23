#!/usr/bin/env python3
import argparse
import sys

import geopandas as gpd
from sqlalchemy import create_engine, text

from geo_assistant.config import Configuration

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
        print(f"Error reading Parquet: {e}", file=sys.stderr)
        sys.exit(1)

    # 2) enforce source CRS if provided
    if args.src_crs:
        gdf = gdf.set_crs(epsg=args.src_crs, allow_override=True)

    # 3) reproject
    gdf = gdf.to_crs(epsg=args.dest_crs)

    # 4) connect
    engine = create_engine(Configuration.db_connection_url)

    # 5) write to PostGIS
    try:
        gdf.to_postgis(
            name=args.table,
            con=engine,
            if_exists=args.if_exists,
            index=args.index
        )
    except Exception as e:
        print(f"Error writing to PostGIS: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(gdf)} features into table '{args.table}'.")

    # 6) create spatial index on the geometry column
    geom_col = gdf.geometry.name  # usually "geometry"
    idx_name = f"idx_{args.table}_{geom_col}"
    create_idx_sql = f"""
    CREATE INDEX IF NOT EXISTS {idx_name}
      ON {args.table}
      USING GIST ({geom_col});
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(create_idx_sql))
            print(f"Created spatial index '{idx_name}' on column '{geom_col}'.")

            query = text(
                (
                    "SELECT Populate_Geometry_Columns("
                    f"'public.{args.table}'::regclass"
                    ");"
                )
            )
            conn.execute(
                query
            )
            print("grant to public")
            conn.execute(
                text(f"GRANT SELECT ON public.{args.table} TO public")
            )

            analyze_sql = f"ANALYZE {args.table};"

            conn.execute(text(analyze_sql))
            print(f"Analyzed table '{args.table}' to update planner statistics.")
    except Exception as e:
        print(f"Error setting up table: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
