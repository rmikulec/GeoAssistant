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
