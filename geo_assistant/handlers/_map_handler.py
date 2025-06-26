import math
from typing import Union
from collections import defaultdict

import plotly.graph_objects as go
from plotly.graph_objects import Figure

from geo_assistant.logging import get_logger
from geo_assistant.table_registry import Table
from geo_assistant.handlers._filter import HandlerFilter
from geo_assistant.config import Configuration

logger = get_logger(__name__)

class PlotlyMapHandler:
    """
    A class used to manage and render Plotly Mapbox figures with vector-tile layers

    Methods:
        - add_map_layer: Adds a vector-tile layer to the map figure
        - remove_map_layer: Removes a layer given its layer_id
        - reset_map: Clears all layers and resets map position
        - update_figure: Applies current state to the figure and returns it
    """

    def __init__(self):
        # Store layers and their filters
        self.map_layers: dict[str, dict] = {}
        self._layer_filters: dict[str, list[HandlerFilter]] = defaultdict(list)
        self._active_table: Table = None

        # Base Figure
        self.figure = go.Figure(go.Choroplethmapbox())  # empty scatter to initialize mapbox
        self.figure.update_layout(
            mapbox=dict(
                style=Configuration.map_box_style,
                center=dict(lat=0, lon=0),
                zoom=1,
                layers=[]
            ),
            margin=dict(l=0, r=0, t=0, b=0)
        )

    @property
    def _global_bounds(self) -> Union[dict[str, float], None]:
        """
        Gets the bounding box for the currently active table, or None if unset
        """
        if self._active_table:
            return self._active_table.bounds
        return None

    def _add_map_layer(
        self,
        table: Table,
        layer_id: str,
        color: str,
        filters: list[HandlerFilter] = None,
        style: str = "line"
    ) -> None:
        """
        Adds or replaces a vector-tile layer on the map.
        """
        url = table.tile_url
        if filters:
            # combine CQL filters
            cql = "%20AND%20".join(f._to_cql() for f in filters)
            url = f"{url}?filter={cql}"

        layer = {
            "sourcetype":"vector",
            "sourcelayer": f"{table.schema}.{table.name}",
            "source":[url],
            "type":style,
            "color": color,
            "type": style,
        }

        self.map_layers[layer_id] = layer
        self._layer_filters[layer_id] = filters or []
        self._active_table = table
        logger.debug(f"Added layer {layer_id}")

    def _remove_map_layer(self, layer_id: str) -> None:
        """
        Removes a specified layer from the map.
        """
        self.map_layers.pop(layer_id, None)
        self._layer_filters.pop(layer_id, None)
        logger.debug(f"Removed layer {layer_id}")

    def _reset_map(self) -> None:
        """
        Clears all layers and resets bounds.
        """
        self.map_layers.clear()
        self._layer_filters.clear()
        self._active_table = None
        logger.debug("Map reset to initial state")

    def update_figure(self) -> Figure:
        """
        Applies current layers and bounds to the figure and returns it.
        """
        # Prepare layout update
        mapbox_config = dict(
            style=Configuration.map_box_style,
            layers=list(self.map_layers.values())
        )

        bounds = self._global_bounds
        if bounds:
            center = {"lon": (bounds["west"] + bounds["east"])/2, "lat": (bounds["south"] + bounds["north"])/2}
            span = max(bounds["east"] - bounds["west"], bounds["north"] - bounds["south"])
            zoom = -math.log2(span / 360)
            mapbox_config.update(center=center, zoom=zoom)

        self.figure.update_layout(mapbox=mapbox_config)
        return self.figure

    @property
    def status(self) -> dict:
        """
        Returns a summary of current layers and filters.
        """
        return {
            lid: {
                "filters": [f.model_dump() for f in flts],
                "layer": lyr
            }
            for lid, (lyr, flts) in zip(self.map_layers.keys(), zip(self.map_layers.values(), self._layer_filters.values()))
        }
