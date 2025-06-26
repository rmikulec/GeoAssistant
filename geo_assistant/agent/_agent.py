import json
from typing import Callable
from sqlalchemy import Engine

# Import for declaring agent
from geo_assistant.agent._base import BaseAgent, tool, tool_type, system_message

# Import geo assisant stuff
from geo_assistant.config import Configuration
from geo_assistant.handlers import PlotlyMapHandler, PostGISHandler
from geo_assistant.table_registry import TableRegistry
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore
from geo_assistant.handlers._filter import HandlerFilter


GEO_AGENT_SYSTEM_MESSAGE = """
You are a geo-assistant who is an expert at making maps in GIS software. You will be given access
to a large dataset of GeoJSON data, and you are tasked to keep the map in a state that best reflects
the conversation with the user.

To do so, you will be given access to the following tools:
  - add_map_layer: You can add a new layer to the map, with the filters and color of your choosing
  - remove_map_layer: You can remove a layer when it is no longer applicable to the conversation
  - reset_map: You can reset the map to have 0 layers and start over

Here is the current status of the map:
{map_status}

Here is any other relevant information:
{context}

Here are the tables that are available:
{tables}
"""


class GeoAgent(BaseAgent):
    def __init__(
            self, 
            engine: Engine, 
            map_handler: PlotlyMapHandler, 
            data_handler: PostGISHandler, 
            field_store: FieldDefinitionStore = None,
            info_store: SupplementalInfoStore = None, 
            model: str = Configuration.inference_model, 
            emitter: Callable=None
        ):
        super().__init__(model, emitter)
        self.engine: Engine = engine
        self.map_handler: PlotlyMapHandler = map_handler
        self.data_handler: PostGISHandler = data_handler
        self.registry: TableRegistry = TableRegistry.load_from_tileserv(self.engine)

        # Set the field store depending if given or not
        if field_store is None:
            self.field_store = FieldDefinitionStore(version=Configuration.field_def_store_version)
        else:
            self.field_store = field_store
        
        # Set the info store depending if given or not
        if info_store is None:
            self.info_store = SupplementalInfoStore(version=Configuration.info_store_version)
        else:
            self.info_store = info_store

    @system_message
    async def _system_message(self, user_message: str):
        # load context and inject as system prompt
        self.registry = TableRegistry.load_from_tileserv(self.engine)
        context = await self.info_store.query(user_message, k=3)
        context = "\n\n".join(r['markdown'] for r in context)
        return GEO_AGENT_SYSTEM_MESSAGE.format(
            map_status=self.map_handler.status,
            context=context,
            tables=list(self.registry.tables.keys())
        )

    def _finalize_response(self, text: str) -> str:
        return text.strip()

    @tool_type(
        name="filter",
        description="One CQL filter clause"
    )
    async def _build_filter_type(self, user_message: str) -> dict[str, dict]:
        """
        Returns a map of JSON-Schema properties for a single filter clause.
        """
        fields = await self.field_store.query(user_message)
        return {
            "field": {
                "type": "string",
                "enum": [f["name"] for f in fields],
                "description": json.dumps(fields),
            },
            "op": {
                "type": "string",
                "enum": [
                    "equal",
                    "greaterThan",
                    "lessThan",
                    "greaterThanOrEqual",
                    "lessThanOrEqual",
                    "notEqual",
                    "contains",
                ],
            },
            "value": {"type": "string"},
        }

    @tool(
        name="add_map_layer",
        description="Add a layer to the map with optional CQL filters",
        params={
            "table":    {"type":"string", "enum": lambda self: [t.name for t in self.registry.tables.values()]},
            "layer_id": {"type":"string"},
            "style":    {"type":"string"},
            "color":    {"type":"string", "description": "A hex value for the color of the layer"},
            "filters":  {"type": "#filter"},
        },
        required=["table","layer_id", "color"],
    )
    def add_map_layer(self, table: str, color: str, layer_id: str, style: str = 'line', filters: list[dict]=None) -> str:
        if filters:
            filters = [
                HandlerFilter(**filter_)
                for filter_ in filters
            ]
        table = self.registry[('table', table)][0]
        self.map_handler._add_map_layer(
            table=table, 
            filters=filters, 
            style=style, 
            color=color, 
            layer_id=layer_id
        )
        count = self.data_handler.filter_count(self.engine, table, filters)
        return f"Layer {layer_id} add with {count} rows"

    @tool(
        name="remove_map_layer",
        description="Remove a layer by its ID",
        params={"layer_id":{"type":"string", "enum": lambda self: list(self.map_handler.map_layers.keys())}},
        required=["layer_id"],
    )
    def remove_map_layer(self, layer_id: str) -> bool:
        self.map_handler._remove_map_layer(layer_id)
        return f"Layer {layer_id} removed from map"

    @tool(
        name="run_analysis",
        description="Perform spatial analysis on the current selection",
        params={"prompt":{"type":"string"}},
        required=["prompt"],
    )
    async def run_analysis(self, prompt: str) -> str:
        # Hand off to specialized analysis logic or agent
        return await super().run_analysis(prompt)