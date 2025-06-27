import json
import pathlib
from typing import Callable, Literal
from sqlalchemy import Engine
from openai.types.responses import ParsedResponse
from jinja2 import Template

# Import for declaring agent
from geo_assistant.agent._base import BaseAgent, tool, tool_type, system_message, postchat
from geo_assistant.agent.updates import EmitUpdate, Status

# Import geo assisant stuff
from geo_assistant.config import Configuration
from geo_assistant.logging import get_logger
from geo_assistant.handlers import PlotlyMapHandler, PostGISHandler
from geo_assistant.table_registry import TableRegistry
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore
from geo_assistant.handlers._filter import HandlerFilter

# Analysis imports
from geo_assistant.agent.analysis import _GISAnalysis
from geo_assistant.agent.analysis.report import TableCreated, PlotlyMapLayerArguements

logger = get_logger(__name__)

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

When the user makes a request:
1. Look at the fields available
2. See what tables the fields are associated with
3. Analyze if the request requires data across tables
    a. If yes, then request an analysis
    b. If no, then add map layers
"""


class FigureUpdate(EmitUpdate):
    """
    New update that sends over an updated plotly json
    """
    type: Literal['figure_update'] = 'figure_update'
    figure: str



class AnalysisUpdate(EmitUpdate):
    type: Literal['analysis'] = 'analysis'
    id: str
    query: str
    step: str
    progress: float


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
        tables = self.registry[('schema', 'base')]
        context = await self.info_store.query(user_message, k=3)
        context = "\n\n".join(r['markdown'] for r in context)
        return GEO_AGENT_SYSTEM_MESSAGE.format(
            map_status=self.map_handler.status,
            context=context,
            tables=[table.name for table in tables]
        )

    async def _emit_figure(self, fig: str):
        if self.emitter:
            await self.emitter(
                FigureUpdate(
                    status=Status.SUCCEDED,
                    figure=json.dumps(fig)
                )
            )

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
            "table":    {"type":"string", "enum": lambda self: [t.name for t in self.registry[('schema', 'base')]]},
            "layer_id": {"type":"string"},
            "style":    {"type":"string"},
            "color":    {"type":"string", "description": "A hex value for the color of the layer"},
            "filters":  {"type": "array", "items": {"type": "#filter"}},
        },
        required=["table","layer_id", "color"],
    )
    async def add_map_layer(self, table: str, color: str, layer_id: str, style: str = 'line', filters: list[dict]=None) -> str:
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
        # Emit the udpated figure
        await self._emit_figure(self.map_handler.update_figure().to_plotly_json())
        count = self.data_handler.filter_count(self.engine, table, filters)
        return f"Layer {layer_id} add with {count} rows"

    @tool(
        name="remove_map_layer",
        description="Remove a layer by its ID",
        params={"layer_id":{"type":"string", "enum": lambda self: list(self.map_handler.map_layers.keys())}},
        required=["layer_id"],
    )
    async def remove_map_layer(self, layer_id: str) -> bool:
        self.map_handler._remove_map_layer(layer_id)
        # Emit the udpated figure
        await self._emit_figure(self.map_handler.update_figure().to_plotly_json())
        return f"Layer {layer_id} removed from map"

    @tool(
        name="reset_map",
        description="Resets the map, removing all layers",
    )
    async def reset_map(self):
        self.map_handler._reset_map()
        # Emit the udpated figure
        await self._emit_figure(self.map_handler.update_figure().to_plotly_json())
        return "Map reseted"

    @tool(
        name="run_analysis",
        description=(
            "Perform an Analysis by querying a PostGIS Database and outputing data"
            "You will have access to SQL Queries, such as Filter, Aggregate, Merge, Buffer"
            "You will have the options to add the results to the map and/or export them"
        ),
        params={"goal":{"type":"string", "description": "Describe the goal of this analysis"}},
        required=["goal"],
    )
    async def run_analysis(self, goal: str):
            """
            Runs an analysis given a user message. This is a more time consuming process than 'chat',
            as it forces the agent to *think* and plan steps, then executes sql to create tables for
            the analysis

            Args:
                - query(str): Text descibing what the analysis should accomplish
            """
            analysis_id = str(abs(hash(goal)))
            if self.emitter:
                await self.emitter(
                    AnalysisUpdate(
                        id=analysis_id,
                        query=goal,
                        step="Generating analysis plan...",
                        status=Status.GENERATING,
                        progress=1
                    )
                )

                
            logger.info(f"Running analysis for query: {goal}")
            # Setup the system message template
            system_message_template = Template(source=pathlib.Path("./geo_assistant/agent/system_message.j2").read_text())
            # Query for relevant fields
            field_defs = await self.field_store.query(goal, k=15)
            field_defs = self.registry.verify_fields(field_defs)
            field_names = [field['name'] for field in field_defs]
            # Query registry for all tables that make up the set of fields
            tables = self.registry[('schema', Configuration.db_base_schema), ('fields', field_names)]
            logger.debug(f"Tables: {tables}")
            # Create a new Analysis Model with those fields as Enums (This forces the model to only
            #   use valid fields)
            DynGISModel = _GISAnalysis.build_model(
                fields=field_names,
                tables=[table.name for table in tables]
            )
            # Query for relative info
            context = await self.info_store.query(goal, k=10)
            # Generate the system message
            system_message = system_message_template.render(
                field_definitions=field_defs,
                context_info=context,
                tables=tables
            )
            logger.debug(system_message)
            
            try:
                # Hit openai to generate a step-by-step plan for the analysis
                res: ParsedResponse[_GISAnalysis] = await self.client.responses.parse(
                    input=[
                        {'role': 'system', 'content': system_message},
                        {'role': 'user', 'content': goal}
                    ],
                    model="o4-mini",
                    reasoning={
                        "effort":"high",
                    },
                    text_format=DynGISModel
                )
                analysis = res.output_parsed
            except Exception as e:
                # Capture any exceptions to emit an error then raise
                if self.emitter:
                    await self.emitter(
                        AnalysisUpdate(
                            id=analysis_id,
                            step="Analysis plan failed to generate.",
                            query=goal,
                            status=Status.ERROR,
                            progress=1.0
                        )
                    )
                    
                raise e
            logger.info(analysis.model_dump_json(indent=2))
            # Run through the steps, executing each query
            logger.debug(f"Steps: {[step.name for step in analysis.steps]}")

            try:
                async def _step_emitter(update_dict: dict):
                    await self.emitter(
                        AnalysisUpdate.model_validate(update_dict)
                    )

                # Execute and gather the report
                report = await analysis.execute(
                    id_=analysis_id, 
                    engine=self.engine, 
                    emitter=_step_emitter,
                    query=goal
                )
                # Perform any actions required based on the report
                for item in report.items:
                    if isinstance(item, TableCreated):
                        table = self.registry.register(
                            id_=f"{analysis.name}.{item.table_created}",
                            engine=self.engine
                        )
                        table._postprocess(self.engine)
                    elif isinstance(item, PlotlyMapLayerArguements):
                        schema, table = item.source_table.split('.')
                        table = self.registry[
                            ('schema', schema),
                            ('table', table)
                        ][0]
                        self.map_handler._add_map_layer(
                            table=table, 
                            color=item.color, 
                            layer_id=item.layer_id
                        )
                        # Emit the udpated figure
                        await self._emit_figure(self.map_handler.update_figure().to_plotly_json())
                    else:
                        logger.warning(
                            f"Report item type {type(item)} handler not implemented"
                        )
                report_succeded = True
            except Exception as e:
                if self.emitter:
                    await self.emitter(
                        AnalysisUpdate(
                            id=analysis_id,
                            query=goal,
                            step="Analysis failed to run.",
                            status=Status.ERROR,
                            progress=1.0
                        )
                    )
                raise e
            finally:
                # No matter what, drop all the tables but the last possible
                logger.debug(analysis.tables_created)
                logger.debug(analysis.final_tables)
                self.registry.sync_tileserv(self.engine)
                for table_name in analysis.tables_created:
                    if table_name not in analysis.final_tables:
                        logger.info(f"Dropping {table_name}...")
                        schema, table = table_name.split('.')
                        self.registry[('schema', schema), ('table', table)][0]._drop(self.engine)
            if self.emitter:
                await self.emitter(
                    AnalysisUpdate(
                        id=analysis_id,
                        query=goal,
                        step="Complete",
                        status=Status.SUCCEDED,
                        progress=1.0
                    )
                )

            return (
                f"GIS Analysis ran succussfully."
                f"Report description:"
                f"{report.model_dump_json(indent=2)}"
            )