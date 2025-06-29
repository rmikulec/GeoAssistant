from geo_assistant.doc_stores._field_definition_store import FieldDefinition
from geo_assistant.handlers import PlotlyMapHandler


def _build_add_layer_def(tables: list[str], field_defs: list[FieldDefinition]) -> dict:
    props = {
        "layer_id": {"type": "string", "description": "ID for the new layer"},
        "color":    {"type": "string", "description": "Hex color"},
        "style":    {
            "type": "string",
            "enum": ["line","fill"],
            "default": "line",
            "description": "Line or fill"
        },
        "table": {
            "type": "string",
            "enum": tables,
            "description": "The name of the table to pull data from"
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

    return {
        "type": "function",
        "name":"add_map_layer",
        "description":"Add a layer to the map",
        "parameters":{
            "type":"object",
            "properties":props,
            "required":["layer_id","color","style", "table"]
        }
    }


def _build_remove_layer_def(map_handler: PlotlyMapHandler) -> dict:
    return {
        "type": "function",
        "name":"remove_map_layer",
        "description":"Remove a layer from the map",
        "parameters":{
            "type":"object",
            "properties":{
                "layer_id":{
                    "type":"string",
                    "enum": list(map_handler.map_layers.keys()),
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


def _build_run_analysis() -> dict:
    return {
        "type": "function",
        "name": "run_analysis",
        "description": (
            "Runs a multi-step GIS analysis given a user query. "
            "The agent will plan out aggregation, merge, buffer, and filter steps, "
            "execute the corresponding SQL to create intermediate tables, "
            "and return the final result."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Text describing what the analysis should accomplish"
                }
            },
            "required": ["query"]
        }
    }
