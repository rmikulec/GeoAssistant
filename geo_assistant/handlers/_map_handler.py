import requests
import threading
from typing import NoReturn
from functools import cached_property

from plotly.graph_objects import Figure
import plotly.express as px

from geo_assistant.handlers._filter import GeoFilter
from geo_assistant.handlers._exceptions import InvalidTileservTableID
from geo_assistant.config import Configuration

class MapHandler:
    """
    A class used in order to change the state of a plotly Map object

    Methods:
        - add_map_layer: Adds a layer to the plotly map figure
        - remove_map_layer: Removed a layer given a layer_id
        - reset_map: Clears all layers and resets map position
    """

    @cached_property
    def _tileserv_index(self):
        """
        Private property to get the index data from the pg-tileserv server
        """
        print(Configuration.pg_tileserv_url)
        return requests.get(
            f"{Configuration.pg_tileserv_url}/index.json"
        ).json()

    def __init__(self, table_id: str, table_name: str):
        if table_id not in self._tileserv_index:
            raise InvalidTileservTableID(table_id)
        else:
            self.table_id = table_id
            self.table_name = table_name


        # Create the figure and adjust the bounds and margins
        self.figure = px.choropleth_map(zoom=3)
        self.figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        self.figure.update_layout(map_bounds=self._default_bounds)
    
        # Key attributes for udpating the figure
        self.map_layers: dict = {}
        self._layer_filters: dict[str, GeoFilter] = {}

        # Udpate the figure once while initializing
        self.figure.update_layout(
            map_style="dark"
        )

        self._lock = threading.Lock()

    @cached_property
    def _tileserve_table(self):
        """
        The direct json data for the table from pg-tileserv
        """
        return requests.get(
            f"{Configuration.pg_tileserv_url}/{self.table_id}.json"
        ).json()
    
    @property
    def _base_tileurl(self):
        """
        Base tile url to be used as a source for vector layers
        """
        return self._tileserve_table['tileurl']+"?columns%20%3D%20%27BBL%27"

    @property
    def _default_bounds(self):
        """
        The default view window for the map. This window is the minimum size in order to view
        all rows in the table at once
        """
        bounds = self._tileserve_table['bounds']
        return {
            "west": bounds[0],
            "east": bounds[2],
            "south": bounds[1],
            "north": bounds[3]
        }
    
    @property
    def _properties(self):
        """
        Properties in the table that can be used in a filter
        """
        return {
            prop["name"]: prop["type"]
            for prop in self._tileserve_table['properties']
        }
    


    def _add_map_layer(self, layer_id: str, color: str, filters: list[GeoFilter], style: str="line"):
        filter_ = "%20AND%20".join([map_filter._to_cql() for map_filter in filters])
        # Create the layer
        layer = {
            "sourcetype": "vector",
            "sourceattribution": "Locally Hosted PLUTO Dataset",
            "source": [
                self._base_tileurl + "&filter=" + filter_
            ],
            "sourcelayer": self.table_id,                  # ← must match your tileset name :contentReference[oaicite:0]{index=0}
            "type": style,                                 # draw lines
            "color": color,
            "below": "traces" 
        }
        with self._lock:
            # Register it to the map
            self.map_layers[layer_id] = layer
            self._layer_filters[layer_id] = filters
    

    def _remove_map_layer(self, layer_id: str) -> str:
        with self._lock:
            del self.map_layers[layer_id]
            del self._layer_filters[layer_id]
        return f"Layer {layer_id} removed from the map"

    def _reset_map(self) -> str:
        with self._lock:
            self.map_layers = {}
            self._layer_filters = {}
        return "All layers removed from map, blank map initialized"

    def update_figure(self) -> NoReturn:
        """
        Updates the figure depending on what layers have been added / removed since last update

        If there are no more layers (a reset or all removed), then it will reset the entire figure

        Returns:
            Figure: The MapHandler's map plotly figure, configured with all the correct layers
        """
        with self._lock:
            layers = list(self.map_layers.values())
            if layers:
                self.figure.update_layout(
                    map_style="dark",
                    map_layers=layers
                )
            else:
                self.figure = px.choropleth_map(zoom=3)
                self.figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
                self.figure.update_layout(map_bounds=self._default_bounds)
                self.figure.update_layout(map_style="dark")
    

    def get_figure(self) -> Figure:
        with self._lock:
            return self.figure
    

    @property
    def status(self):
        layers = []

        with self._lock:
            for layer_id, layer in self.map_layers.items():
                filters = self._layer_filters[layer_id]
                layers.append(
                    {
                        "id": layer_id,
                        "color": layer['color'],
                        "style": layer['type'],
                        "filters": [filter_.model_dump() for filter_ in filters]
                    }

                )
        
        return layers
