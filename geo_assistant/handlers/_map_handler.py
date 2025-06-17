import requests
from functools import cached_property

import plotly.express as px


from geo_assistant.handlers._filter import GeoFilter


class MapHandler:
    """
    A class used in order to change the state of a plotly Map object

    Methods:
        - add_table
        - remove_table
        - reset_tables
        - get_current_state
    """

    @cached_property
    def _tileserv_index(self):
        """
        Private property to get the index data from the pg-tileserv server
        """
        return requests.get(
            "http://localhost:7800/index.json"
        ).json()

    def __init__(self, table_id: str, table_name: str):
        if table_id not in self._tileserv_index:
            raise Exception(f"table with ID {table_id} not found in pg-tileserv index. Please check http://localhost:7800/index.json")
        else:
            self.table_id = table_id
            self.table_name = table_name


        # Create the figure and adjust the bounds and margins
        self.figure = px.choropleth_map(zoom=3)
        self.figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        self.figure.update_layout(map_bounds=self._default_bounds)
    
        # Key attributes for udpating the figure
        self.map_layers = {}
        self._layer_filters = {}

        # Udpate the figure once while initializing
        self.figure.update_layout(
            map_style="dark"
        )

    @cached_property
    def _tileserve_table(self):
        """
        The direct json data for the table from pg-tileserv
        """
        return requests.get(
            f"http://localhost:7800/{self.table_id}.json"
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
    


    def _add_map_layer(self, layer_id: str, color: str, filters: list[GeoFilter], type_: str="line"):
        filter_ = "&".join([map_filter._to_cql() for map_filter in filters])
        # Create the layer
        layer = {
            "sourcetype": "vector",
            "sourceattribution": "Locally Hosted PLUTO Dataset",
            "source": [
                self._base_tileurl + "&filter=" + filter_
            ],
            "sourcelayer": self.table_id,                   # ← must match your tileset name :contentReference[oaicite:0]{index=0}
            "type": type_,                                 # draw lines
            "color": color,
            "below": "traces" 
        }
        # Register it to the map
        self.map_layers[layer_id] = layer
        self._layer_filters[layer_id] = filters
    

    def _remove_map_layer(self, layer_id: str):
        del self.map_layers[layer_id]
        del self._layer_filters[layer_id]


    def _reset_map(self):
        self.map_layers = {}
        self._layer_filters = {}

    def update_figure(self):
        self.figure.update_layout(
            map_style="dark",
            map_layers=list(self.map_layers.values())
        )
        return self.figure