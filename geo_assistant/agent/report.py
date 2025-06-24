from pydantic import BaseModel
from typing import Union

class TableCreated(BaseModel):
    name: str
    reason: str
    table: str
    is_intermediate: bool


class MapLayerCreated(BaseModel):
    name: str
    reason: str
    source_table: str
    layer_id: str
    color: str


class GISReport(BaseModel):
    items: list[Union[TableCreated, MapLayerCreated]]