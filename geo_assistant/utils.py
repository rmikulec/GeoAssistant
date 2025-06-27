import geopandas as gpd
from shapely.geometry.base import BaseGeometry

def pick_best_geometry(
    gdf: gpd.GeoDataFrame,
    priority: list[str] = ["Polygon", "LineString", "Point"],
    convert: bool = False
) -> gpd.GeoDataFrame:
    """
    Choose the dominant geometry type in gdf (by count), then either:
      - subset to only that type (convert=False), or
      - convert all geometries into that type (convert=True).

    Args:
        - gdf (GeoDataFrame): Dataframe to inspect
        - priority (list[str]): search order for types if there's a tie or explicit preference.
            Defaults to ["Polygon", "Linestring", "Point"]
        - convert (bool): if True, transforms all rows into chosen type by using on of these
            methods: (centroid/boundary/envelope). Defaults to False
    """
    # map Shapely types into our three “buckets”
    type_map = {
        "Point":      "Point",     "MultiPoint":  "Point",
        "LineString": "LineString","MultiLineString":"LineString",
        "Polygon":    "Polygon",   "MultiPolygon": "Polygon",
    }

    # map each row to bucket (others become “Other”)
    mapped = gdf.geom_type.map(type_map).fillna("Other")

    # count them
    counts = mapped.value_counts()
    if counts.empty:
        raise ValueError("GeoDataFrame contains no geometries")

    # pick best by priority, else highest count
    for geom in priority:
        if geom in counts:
            best = geom
            break
    else:
        best = counts.idxmax()

    if not convert:
        # just return the subset of that geometry
        return gdf[mapped == best].copy()

    # else: convert all geometries into `best`
    new_geom: list[BaseGeometry]
    if best == "Point":
        new_geom = [geom.centroid for geom in gdf.geometry]
    elif best == "LineString":
        new_geom = [geom.boundary for geom in gdf.geometry]
    elif best == "Polygon":
        # envelope = bounding box polygon; buffer(0) can also fix minor invalids
        new_geom = [geom.envelope for geom in gdf.geometry]
    else:
        # fallback: don’t change
        new_geom = list(gdf.geometry)

    out = gdf.copy()
    out.set_geometry(new_geom, inplace=True)
    return out

