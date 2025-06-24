import requests
from functools import cached_property
from collections import defaultdict

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
    def _tileserv_index(self) -> dict:
        """
        Private property to get the index data from the pg-tileserv server
        """
        return requests.get(
            f"{Configuration.pg_tileserv_url}/index.json"
        ).json()

    def __init__(self, default_table: str = Configuration.default_table):
        # Key attributes for udpating the figure
        self.map_layers: dict = {}
        self._layer_filters: dict[str, list[GeoFilter]] = defaultdict(list)
        self._layer_ids: dict[str, list[str]] = defaultdict(list)
        self._active_table: str = "base." + default_table
    
        # Create the figure and adjust the bounds and margins
        self.figure = px.choropleth_map(zoom=3)
        self.figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        self.figure.update_layout(map_bounds=self._global_bounds)

        # Udpate the figure once while initializing
        self.figure.update_layout(
            map_style="dark"
        )

    def _get_table_metadata(self, table_id: str):
        """
        The direct json data for the table from pg-tileserv
        """
        try:
            print(table_id)
            return requests.get(
                f"{Configuration.pg_tileserv_url}/{table_id}.json"
            ).json()
        except requests.ConnectionError:
            raise InvalidTileservTableID(table_id=table_id)


    def _get_base_tileurl(self, table_id: str):
        """
        Base tile url to be used as a source for vector layers
        """
        return self._get_table_metadata(table_id)['tileurl']+"?columns%20%3D%20%27BBL%27"

    @property
    def _global_bounds(self):
        if self._active_table:
            bounds = self._get_table_metadata(self._active_table)['bounds']
            return {
                "west": bounds[0],
                "south": bounds[1],
                "east":  bounds[2],
                "north": bounds[3],
            }
        else:
            return {
                "west": -90,
                "south": -180,
                "east":  90,
                "north": 180,
            }
    

    def _add_map_layer(self, table: str, layer_id: str, color: str, filters: list[GeoFilter] = None, style: str="line"):
        """
        Private method to add a new layer to the map. Layers consist of filters and are automatically
        split by 'table'
        """
        if filters:
            sorted_filters = defaultdict(list)
            for filter_ in filters:
                sorted_filters[filter_.table].append(filter_)

            for table, filters_ in sorted_filters.items():
                filter_ = "%20AND%20".join([map_filter._to_cql() for map_filter in filters_])
                # Create the layer
                layer = {
                    "sourcetype": "vector",
                    "sourceattribution": "Locally Hosted PLUTO Dataset",
                    "source": [
                        self._get_base_tileurl(table) + "&filter=" + filter_
                    ],
                    "sourcelayer": table,                  # ← must match your tileset name :contentReference[oaicite:0]{index=0}
                    "type": style,                                 # draw lines
                    "color": color,
                    "below": "traces" 
                }
                # Register it to the map
                table_layer_id = f"{table}.{layer_id}"
                self.map_layers[table_layer_id] = layer
                self._layer_ids[layer_id].append(table_layer_id)
        else:
            layer = {
                "sourcetype": "vector",
                "sourceattribution": "Locally Hosted PLUTO Dataset",
                "source": [
                    self._get_base_tileurl(table)
                ],
                "sourcelayer": table,                  # ← must match your tileset name :contentReference[oaicite:0]{index=0}
                "type": style,                                 # draw lines
                "color": color,
                "below": "traces" 
            }
            table_layer_id = f"{table}.{layer_id}"
            self.map_layers[table_layer_id] = layer
            self._layer_ids[layer_id].append(table_layer_id)

        # Add all filters to layer_filters dict, keeping them together
        self._layer_filters[layer_id] = filters
        self._active_table = table
        print(self.map_layers)
    

    def _remove_map_layer(self, layer_id: str) -> str:
        """
        Removes a layer from the map
        """
        for table_layer_id in self._layer_ids[layer_id]:
            del self.map_layers[table_layer_id]
        del self._layer_filters[layer_id]
        del self._layer_ids[layer_id]
        return f"Layer {layer_id} removed from the map"

    def _reset_map(self) -> str:
        """
        Removes all layers on the map
        """
        self.map_layers = {}
        self._layer_filters = {}
        return "All layers removed from map, blank map initialized"

    def update_figure(self) ->Figure:
        """
        Updates the figure depending on what layers have been added / removed since last update

        If there are no more layers (a reset or all removed), then it will reset the entire figure

        Returns:
            Figure: The MapHandler's map plotly figure, configured with all the correct layers
        """
        layers = list(self.map_layers.values())
        if layers:
            self.figure.update_layout(
                map_style="dark",
                map_layers=layers
            )
        else:
            self.figure.update_layout(map_style="dark")

        return self.figure

    @property
    def status(self):
        """
        Status so far consists of a readable dict, of all the layers and their filters on the map
        """
        layers = []

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
