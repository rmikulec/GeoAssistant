from typing import Union, Literal
from pydantic import BaseModel, Field, ConfigDict, create_model

# supported spatial aggregation functions
SpatialOperator = Literal[
    'COLLECT',      # ST_Collect
    'UNION',        # ST_Union
    'CENTROID',     # ST_Centroid
    'EXTENT',       # ST_Extent
    'ENVELOPE',     # ST_Envelope
    'CONVEXHULL'    # ST_ConvexHull
]

class SpatialAggregator(BaseModel):
    operator: SpatialOperator = Field(..., description="Spatial aggregation function")
    column:   str             = Field(
        'geometry',
        description="Geometry column to aggregate"
    )
    alias:    str | None      = Field(
        None,
        description="Optional alias for the aggregated geometry"
    )

    # use operator as discriminator
    model_config = ConfigDict(discriminator='operator')

    @classmethod
    def _build_aggregator(cls, fields_enum):
        return create_model(
            f"{cls.__name__}Dynamic",
            __base__=cls,
            column=(fields_enum, ...)
        )



class CollectSpatial(SpatialAggregator):
    operator: Literal['COLLECT']
    # no extra props


class UnionSpatial(SpatialAggregator):
    operator: Literal['UNION']
    # no extra props


class CentroidSpatial(SpatialAggregator):
    operator: Literal['CENTROID']
    # no extra props


class ExtentSpatial(SpatialAggregator):
    operator: Literal['EXTENT']
    # note: ST_Extent returns BOX2D text, not geometry


class EnvelopeSpatial(SpatialAggregator):
    operator: Literal['ENVELOPE']
    # no extra props


class ConvexHullSpatial(SpatialAggregator):
    operator: Literal['CONVEXHULL']
    # no extra props


SQLSpatialAggregators: list[SpatialAggregator] = [
    CollectSpatial,
    UnionSpatial,
    CentroidSpatial,
    ExtentSpatial,
    EnvelopeSpatial,
    ConvexHullSpatial,
]
