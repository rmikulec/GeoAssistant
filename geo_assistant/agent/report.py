"""
Pydantic classes for the final output of a GIS Analysis. This is meant to be read / interpreted
    in different ways, leaving open the door for future uses.
"""

from pydantic import BaseModel
from typing import Union

class TableCreated(BaseModel):
    """
    Details the table that was created during execution
    """
    name: str
    reason: str
    table_created: str


class PlotlyMapLayerArguements(BaseModel):
    """
    Contains arguements that can be sent to plotly to add a vector layer to the Map
    """
    name: str
    reason: str
    source_table: str
    layer_id: str
    color: str


class SaveTable(BaseModel):
    table: str
    schema: str


class GISReport(BaseModel):
    items: list[Union[TableCreated, PlotlyMapLayerArguements]]