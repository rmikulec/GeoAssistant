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


class DashLeafletMapHandler:
    """
    Builds up a Dash-Leaflet map configuration as JSON.

    Methods:
        - add_map_layer: register a new tile/vector layer
        - remove_map_layer: drop a layer by ID
        - reset_map: clear all layers
        - get_map_json: emit {"center", "zoom", "children": [...]}
    """

    def __init__(self):
        # store layer definitions keyed by layer_id
        self.map_layers: dict[str, dict] = {}
        self.map_layers['default'] = {
            "type": "TileLayer",
            "props": {
                "url": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png",
                "attribution": "&copy; OpenStreetMap contributors"
            }
        }
        # track filters per layer for status reporting
        self._layer_filters: dict[str, list[HandlerFilter]] = defaultdict(list)
        self._active_table: Table = None

    @property
    def _global_bounds(self) -> Union[dict[str, float], None]:
        """Calculate bounds from the current active table if any."""
        if self._active_table:
            return self._active_table.bounds
        return None

    def _add_map_layer(
        self,
        table: Table,
        layer_id: str,
        filters: list[HandlerFilter] = None,
        attribution: str = "&copy; OpenStreetMap contributors",
        default_url: str = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        *args,
        **kwargs
    ) -> str:
        """
        Register a new raster-tile layer for dash-leaflet.

        Produces a dict that mimics:
        dl.TileLayer(url=…, attribution=…)
        """

        # Determine which URL to use: table.tile_url w/ optional CQL filter, or default OSM
        if getattr(table, "tile_url", None):
            url = table.tile_url
            # append CQL if provided
            if filters:
                cql = "%20AND%20".join(f._to_cql() for f in filters)
                url = f"{url}?filter={cql}"
        else:
            # no tileserver on table → use default OpenStreetMap tiles
            url = default_url

        # Build the JSON descriptor for a dl.TileLayer
        layer_def = {
            "type": "VectorTileLayer",
            "props": {
                "url": url,
                "attribution": attribution,
                "style": {},
                "crossOrigin": ""  # or "anonymous"
            }
        }

        # Register it
        self.map_layers[layer_id] = layer_def
        # Keep filters around in case you need to inspect them later
        self._layer_filters[layer_id] = filters or []
        # Activate this table only if it actually had a tile_url
        if getattr(table, "tile_url", None):
            self._active_table = table

        return f"Added raster layer {layer_id}"


    def _remove_map_layer(self, layer_id: str) -> str:
        """Drop a layer by its ID."""
        if layer_id in self.map_layers:
            self.map_layers.pop(layer_id)
            self._layer_filters.pop(layer_id, None)
            logger.debug(f"Removed layer {layer_id}")
            return f"Removed {layer_id}"
        else:
            return f"No such layer: {layer_id}"

    def _reset_map(self) -> str:
        """Clear all layers and reset state."""
        self.map_layers.clear()
        self._layer_filters.clear()
        self._active_table = None
        logger.debug("Reset map state")
        return "Map reset"

    def _compute_viewport(self) -> dict:
        """
        Turn bounds into a center + zoom for Leaflet.
        Leaflet zoom ≈ −log2(span/360)
        """
        bounds = self._global_bounds
        if not bounds:
            # default to world view
            return {"center": [0, 0], "zoom": 2}
        west, east = bounds["west"], bounds["east"]
        south, north = bounds["south"], bounds["north"]
        center = [(south + north) / 2, (west + east) / 2]
        span = max(east - west, north - south)
        zoom = -math.log2(span / 360)
        return {"center": center, "zoom": zoom}

    def update_figure(self) -> dict:
        """
        Emit the full JSON payload suitable for:

        dl.Map(
            center=payload["center"],
            zoom=payload["zoom"],
            children=[ getattr(dl, L["type"])(**L["props"]) for L in payload["children"] ]
        )

        or for a React client to consume directly.
        """
        viewport = self._compute_viewport()
        children = list(self.map_layers.values())
        return {
            **viewport,
            "children": children
        }

    @property
    def status(self) -> list[dict]:
        """A simple JSON list of current layers & their filters."""
        out = []
        for lid, layer in self.map_layers.items():
            entry = {
                "id": lid,
                "type": layer["type"],
                **{k: v for k, v in layer["props"].items() if k in ("color", "style")}
            }
            filters = self._layer_filters.get(lid)
            if filters:
                entry["filters"] = [f.model_dump() for f in filters]
            out.append(entry)
        return out