from geo_assistant.data_dictionary import FieldDefinition
from geo_assistant.handlers import MapHandler

import json

def _build_add_layer_def(field_defs: list[FieldDefinition]) -> dict:
    props = {
        "layer_id": {"type": "string", "description": "ID for the new layer"},
        "color":    {"type": "string", "description": "Hex color"},
        "style":    {
            "type": "string",
            "enum": ["line","fill"],
            "default": "line",
            "description": "Line or fill"
        }
    }

    for fd in field_defs:
        props[fd["name"]] = {
            "type": "object",
            "properties": {
                "value":  {"type": fd["format"]},
                "operator": {
                    "type": "string",
                    "enum": [
                        "equal","greaterThan","lessThan",
                        "greaterThanOrEqual","lessThanOrEqual","notEqual"
                    ] if fd["format"] != "string" else ["equal", "notEqual", "contains"]
                }
            },
            "required": ["value","operator"],
            "description": fd["description"]
        }

    print(json.dumps(props, indent=2))

    return {
        "type": "function",
        "name":"add_map_layer",
        "description":"Add a layer to the map",
        "parameters":{"type":"object","properties":props,"required":["layer_id","color","style"]}
    }


def _build_remove_layer_def(map_handler: MapHandler) -> dict:
    return {
        "type": "function",
        "name":"remove_map_layer",
        "description":"Remove a layer from the map",
        "parameters":{
            "type":"object",
            "properties":{
                "layer_id":{
                    "type":"string",
                    "enum": list(map_handler._layer_filters.keys()),
                    "description":"ID of the layer to remove"
                }
            },
            "required":["layer_id"]
        }
    }

def _build_reset_def() -> dict:
    return {
        "type": "function",
        "name":"reset_map",
        "description":"Reset the map (remove all layers)",
        "parameters":{"type":"object","properties":{}, "required":[]}
    }
