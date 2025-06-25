import math
import json
from typing import Union
from geo_assistant.logging import get_logger
from collections import defaultdict

from plotly.graph_objects import Figure
import plotly.express as px

from geo_assistant.table_registry import Table
from geo_assistant.handlers._filter import HandlerFilter
from geo_assistant.handlers._exceptions import InvalidTileservTableID
from geo_assistant.config import Configuration

logger = get_logger(__name__)

class PlotlyMapHandler:
    """
    A class used in order to change the state of a plotly Map object

    Methods:
        - add_map_layer: Adds a layer to the plotly map figure
        - remove_map_layer: Removed a layer given a layer_id
        - reset_map: Clears all layers and resets map position
    """

    def __init__(self):
        # Key attributes for udpating the figure
        self.map_layers: dict = {}
        self._layer_filters: dict[str, list[HandlerFilter]] = defaultdict(list)
        self._layer_ids: dict[str, list[str]] = defaultdict(list)

        # Set active table to None when loading application
        self._active_table: Table = None
    
        # Create the figure and adjust the bounds and margins
        self.figure = px.choropleth_map(zoom=2)

        # 4) One single update that sets margins, style, and bounds
        self.figure.update_layout(
            margin=dict(r=0, t=0, l=0, b=0),
            map_style="dark",
        )

        if self._global_bounds:
            self.figure.update_layout(
                map_bounds= self._global_bounds
            )

    @property
    def _global_bounds(self) -> Union[dict[str, float], None]:
        """
        Gets the global bounds based on a given table. If none, will return the bounds as
            the entire maps
        """
        if self._active_table:
            return self._active_table.bounds
        else:
            return None
    

    def _add_map_layer(self, table: Table, layer_id: str, color: str, filters: list[HandlerFilter] = None, style: str="line") -> str:
        """
        Private method to add a new layer to the map. Layers consist of filters and are automatically
        split by 'table'
        """
        if filters:
            cql_filter = "%20AND%20".join([filter_._to_cql() for filter_ in filters])
            # Create the layer
            layer = {
                "sourcetype": "vector",
                "sourceattribution": "Locally Hosted PLUTO Dataset",
                "source": [
                    table.tile_url + "?filter=" + cql_filter
                ],
                "sourcelayer": f"{table.schema}.{table.name}",                  # ← must match your tileset name
                "type": style,                                 # draw lines
                "color": color,
                "below": "traces" 
            }
            # Register it to the map
            self.map_layers[layer_id] = layer
        else:
            layer = {
                "sourcetype": "vector",
                "sourceattribution": "Locally Hosted PLUTO Dataset",
                "source": [
                    table.tile_url
                ],
                "sourcelayer": f"{table.schema}.{table.name}",                  # ← must match your tileset name
                "type": style,                                 # draw lines
                "color": color,
                "below": "traces" 
            }
            self.map_layers[layer_id] = layer

        # Add all filters to layer_filters dict, keeping them together
        self._layer_filters[layer_id] = filters
        self._active_table = table
        return f"Added {layer_id}: {layer} to map"
    

    def _remove_map_layer(self, layer_id: str) -> str:
        """
        Removes a layer from the map
        """
        self.map_layers.pop(layer_id)
        self._layer_filters.pop(layer_id)
        logger.debug(f"Removed layer: {layer_id}")
        return f"Layer {layer_id} removed from the map"

    def _reset_map(self) -> str:
        """
        Removes all layers on the map
        """
        self.map_layers = {}
        self._layer_filters = {}
        self._active_table = None
        logger.debug("Reset map to blank state")
        return "All layers removed from map, blank map initialized"

    def update_figure(self) -> Figure:
        layers = list(self.map_layers.values())
        # reuse whatever style you’ve chosen; you could even track it in self._style
        style = "dark"

        # build the kwargs once
        bounds = self._global_bounds
        if bounds:
            center = {"lon": (bounds["west"] + bounds["east"])/2, "lat": (bounds["south"] + bounds["north"])/2}
            span = max(bounds["east"] - bounds["west"], bounds["north"] - bounds["south"])
            zoom = -math.log2(span/360)
            layout_kwargs = {
                "map_style": style,
                "map_bounds": self._global_bounds,
                "map_center": center,
                "map_zoom": zoom,
            }
            if layers:
                logger.info(layers)
                layout_kwargs["map_layers"] = layers
            else:
                layout_kwargs["map_layers"] = []

            self.figure.update_layout(**layout_kwargs)
        return self.figure

    @property
    def status(self) -> dict:
        """
        Status so far consists of a readable dict, of all the layers and their filters on the map
        """
        layers = []

        for layer_id, layer in self.map_layers.items():
            filters = self._layer_filters[layer_id]
            layer_json = {
                "id": layer_id,
                "color": layer['color'],
                "style": layer['type'],
            }
            if filters:
                layer_json["filters"]= [filter_.model_dump() for filter_ in filters]
            layers.append(layer_json)
        return layers
