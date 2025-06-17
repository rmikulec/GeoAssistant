import geopandas as gpd
import pandas as pd
import pathlib
import openai
import json


from geo_assistant.handlers import MapHandler, DataHandler, GeoFilter


GEO_AGENT_SYSTEM_MESSAGE = """
You are a geo-assistant who is an expert at making maps in GIS software. You will be given access
to a large dataset of GeoJSON data, and you are tasked to keep the map in a state that best reflects
the conversation with the user.

To do so, you will be given access to the following tools:
  - add_map_layer: You can add a new layer to the map, with the filters and color of your choosing
  - remove_map_layer: You can remove a layer when it is no longer applicable to the conversation
  - reset_map: You can reset the map to have 0 layers and start over
"""


class GeoAgent:
    """
    OpenAI powered agent to filter parcels and answer user questions

    Methods:
        - chat(user_message: str, field_defs: list[dict]): Function used to 'chat' with the
            GeoAgent. To use, must pass a user-message and a list of field definitions. Chat will
            then use OpenAI SDK to select proper filters (based on the field definitions) and
            answer the user in a conversational way
    """

    def __init__(
        self, 
        map_handler: MapHandler,
        data_handler: DataHandler,
        model: str = "gpt-4o",
    ):
        self.model = model
        self.map_handler = map_handler
        self.data_handler = data_handler
        self.client = openai.OpenAI(api_key=pathlib.Path("./openai.key").read_text())

        self.messages = [
            {'role': 'system', 'content': GEO_AGENT_SYSTEM_MESSAGE}
        ]


    def _build_add_map_layer_func_def(self, field_defs: list[dict]):
        """
        Private method to build an OpenAI Function JSON schema, based on a given set of field
            definitions
        """
        func_def = {
            "type": "function",
            "name": "add_map_layer",
            "description": "Adds a new layer to the map",
            "parameters": {
                "type": "object",
                "properties": {
                    "color": {
                        "type": "string",
                        "description": "Hex value of the color for this layer",
                    },
                    "layer_id": {
                        "type": "string",
                        "description": "A code friendly id for the layer"
                    },
                    "style": {
                        "type": "string",
                        "description": "The style of the layer, can be outlined ('line') for filled in ('fill')",
                        "enum": ['line', 'fill'],
                        "default": "line"
                    }
                },
                "required":["color", "layer_id", "style"]
            }
        }

        for res in field_defs:
            func_def['parameters']['properties'][res['name']] = {
                "type": "object",
                "description": res['description'],
                "properties":
                    {
                        "value": {
                            "type": res['format'],
                        },
                        "operator": {
                            "type": "string",
                            "enum": ["equal", "greaterThan", "lessThan", "greaterThanOrEqual", "lessThanOrEqual", "notEqual", "IGNORE"],
                            "default": "IGNORE"
                        }
                    },
                    "required": ["value"]
            }
            func_def['parameters']['required'].append(res['name'])

        return func_def
    
    def _build_remove_map_layer_func_def(self):
        func_def = {
            "type": "function",
            "name": "remove_map_layer",
            "description": "Remove a layer from the map",
            "parameters": {
                "type": "object",
                "properties": {
                    "layer_id": {
                        "type": "string",
                        "description": "The layer to remove from the map",
                        "enum": list(self.map_handler._layer_filters.keys())
                    }
                },
                "required":["layer_id"]
            }
        }
        return func_def

    def _build_reset_map_func_def(self):
        func_def = {
            "type": "function",
            "name": "reset_map",
            "description": "Adds a new layer to the map",
        }

        return func_def
    

    def chat(self, user_message: str, field_defs: list[dict]) -> str:
        """
        Function used to 'chat' with the
            GeoAgent. To use, must pass a user-message and a list of field definitions. Chat will
            then use OpenAI SDK to select proper filters (based on the field definitions) and
            answer the user in a conversational way

        Args:
            user_message(str): The message inputed by the user
            field_defs(list[dict]): Field Definitions that can be used to filter the GeoDataframe
                These can be obtained by querying the "FieldDefinitionStore"
        Returns:
            str: The message generated by the LLM to return to the user
        """
        self.messages.append(
            {'role': 'user', 'content': user_message}
        )

        res = self.client.responses.create(
            model=self.model,
            input=self.messages,
            tools=[
                self._build_add_map_layer_func_def(field_defs=field_defs),
                self._build_remove_map_layer_func_def(),
                self._build_reset_map_func_def()
            ]
        )

        made_tool_calls = False
        for tool_call in res.output:
            self.messages.append(tool_call)
            if tool_call.type != "function_call":
                continue

            made_tool_calls = True
            args = json.loads(tool_call.arguments)

            print(f"Calling {tool_call.name} with filters: {args}")

            match tool_call.name:

                case "add_map_layer":   
                    layer_id=args.pop('layer_id')
                    color=args.pop('color')
                    type_=args.pop('style')
                    filters = [
                        GeoFilter(
                            field=filter_name,
                            value=filter_details['value'],
                            op=filter_details['operator']
                        )
                        for filter_name, filter_details in args.items()
                        if filter_details['operator'] != "IGNORE"
                    ]    
                    self.map_handler._add_map_layer(
                        layer_id=layer_id,
                        color=color,
                        type_=type_,
                        filters=filters
                    )
                    self.messages.append({
                        "type": "function_call_output",
                        "call_id": tool_call.call_id,
                        "output": f"{self.data_handler.filter_count(filters)} parcels found"
                    })
                case "remove_map_layer":
                    layer_id = args.pop('layer_id')
                    self.map_handler._remove_map_layer(layer_id)
                    self.messages.append({
                        "type": "function_call_output",
                        "call_id": tool_call.call_id,
                        "output": f"Layer {layer_id} removed from map."
                    })
                case "reset_map":
                    self.map_handler._reset_map()
                    self.messages.append({
                        "type": "function_call_output",
                        "call_id": tool_call.call_id,
                        "output": f"All layers removed from the map."
                    })
        if made_tool_calls:
            res = self.client.responses.create(
                model=self.model,
                input=self.messages,
            )
        
        
        ai_message = res.output[0].content[0].text
        self.messages.append({'role': 'assistant', 'content': ai_message})
        return ai_message