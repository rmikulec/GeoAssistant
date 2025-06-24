"""
GeoAgent: An LLM-driven agent to manage a map and build/run GIS Analytics
"""

from dotenv import load_dotenv
load_dotenv()

from geo_assistant.logging import get_logger
import pathlib
import openai
import json

from openai.types.responses import ParsedResponse
from typing import Callable, Any
from jinja2 import Template
from sqlalchemy import Engine

from geo_assistant.handlers import PlotlyMapHandler, PostGISHandler, HandlerFilter
from geo_assistant.agent.report import TableCreated, PlotlyMapLayerArguements
from geo_assistant.table_registry import TableRegistry
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore
from geo_assistant import tools
from geo_assistant.config import Configuration

from geo_assistant.agent.analysis import _GISAnalysis
from geo_assistant.agent._steps import _AggregateStep, _FilterStep, _MergeStep, _BufferStep, _PlotlyMapLayerStep


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
{supplement_information}
"""


class GeoAgent:
    """
    OpenAI powered agent to filter parcels and answer user questions

    Methods:
        - chat(user_message: str, field_defs: list[dict]): Function used to 'chat' with the
            GeoAgent. To use, must pass a user-message and a list of field definitions. Chat will
            then use OpenAI SDK to select proper filters (based on the field definitions) and
            answer the user in a conversational way
        - run_analysis(query: str): Builds and runs a analysis by preplaning a variety of steps,
            querying databases, and building a report to be interpreted by the agent or returned
            to the user

    Args:
        engine (Engine): A sqlaclemeny engine for querying the database for info and running
            analysis to build tables
        map_handler (MapHandler): Class used to manage the state of a plotly map box
        data_handler (DataHandler): Class used to quickly query the database for relevant info
        field_store (FieldDefinitionStore): Field Definition Vector store to ensure relevant field
            definitions are injected into the prompt on each run. If left blank, will default to
            the version found in `geo_assistant.config`
        info_store (SupplementalInfoStore): Info vector store to ensure relevant generic
            information (usually parsed from a GIS dataset metadata pdf) is injected into the
            prompt. If left blank, will default to the version found in `geo_assistant.config`
        model (str): The name of the OpenAI model to use for inference. If left blank, will default
            to the version found in `geo_assistant.config`
        use_smart_search (bool): Flag to use a "smart search" feature. If true, this will enable
            the agent to call OpenAI to generate keywords to search the vector stores with, rather
            than relying on the base user message. The keywords generated use the entire
            conversation as context, potentially catching tricker use cases. Defaults to False.
    """

    def __init__(
        self, 
        engine: Engine,
        map_handler: PlotlyMapHandler,
        data_handler: PostGISHandler,
        field_store: FieldDefinitionStore = None,
        info_store: SupplementalInfoStore = None,
        model: str = Configuration.inference_model,
        use_smart_search: bool = False
    ):
        self.model: str = model
        self.engine: Engine = engine
        self.use_smart_search: bool = use_smart_search
        self.map_handler: PlotlyMapHandler = map_handler
        self.data_handler: PostGISHandler = data_handler
        self.client: openai.AsyncOpenAI = openai.AsyncOpenAI(api_key=pathlib.Path("./openai.key").read_text())
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

        # Iniate a base system message (this will be updated each run of `chat`)
        self.messages = [
            {'role': 'developer', 'content': GEO_AGENT_SYSTEM_MESSAGE.format(
                    map_status="Graph not updated yet.",
                    supplement_information=""
                )    
            }
        ]

        # Set the tool handlers for any potential available tools
        self.tools: dict[str, Callable[..., Any]] = {
            "add_map_layer":    self.map_handler._add_map_layer,
            "remove_map_layer": self.map_handler._remove_map_layer,
            "reset_map":        self.map_handler._reset_map,
            "run_analysis":     self.run_analysis
        }


    @property
    def conversation(self):
        """
        A user-friendly export of the conversation so far, leaving out any `tool calls`
        """
        conversation_str = ""
        for message in self.messages:
            if not isinstance(message, dict):
                # This will skip any tool messages
                continue
            if message.get('role', None) == 'assistant':
                conversation_str+=f"\n GeoAssist: {message['content']}"
            elif message.get('role', None) == 'user':
                conversation_str+=f"\n User: {message['content']}"
        return conversation_str
    

    def _update_dev_message(self, context: str):
        """
        Private function to update the current system message
        """
        self.messages[0] = {'role': 'developer', 'content': GEO_AGENT_SYSTEM_MESSAGE.format(
                map_status=json.dumps(self.map_handler.status,indent=2),
                supplement_information=context
            )    
        }
    

    async def _get_field_defs(self, message: str, context: str = None, k: int = 5):
        """
        Private method to retrieve relevant field definitions from the vector store
        """
        conversation = self.conversation + f"\n User: {message}"
        if self.use_smart_search:
            field_defs = await self.field_store.smart_query(
                message,
                conversation=conversation,
                context=context,
                k=k
            )
        else:
            field_defs = await self.field_store.query(message, k=k)
        return field_defs


    async def chat(self, user_message: str) -> str:
        """
        Function used to 'chat' with the
            GeoAgent. To use, must pass a user-message and a list of field definitions. Chat will
            then use OpenAI SDK to select proper filters (based on the field definitions) and
            answer the user in a conversational way

        Args:
            user_message(str): The message inputed by the user
        Returns:
            str: The message generated by the LLM to return to the user
        """
        self.registry = TableRegistry.load_from_tileserv(self.engine)
        # Add the user message to the message log
        self.messages.append({'role': 'user', 'content': user_message})
        logger.debug(f"User message: {user_message}")

        # Search info vector store and update dev message
        context_search = await self.info_store.query(user_message, k=3)
        context = "\n".join([result['markdown'] for result in context_search])
        self._update_dev_message(context)
        # Search the field vector store and pull out a list of tables
        field_defs = await self._get_field_defs(user_message, context)
        field_defs = self.registry.verify_fields(field_defs)
        field_names = [field_def['name'] for field_def in field_defs]
        tables = self.registry[('schema', Configuration.db_base_schema), ('fields', field_names)]
        # Build out available tools
        # TODO: Make this more robust and easier to add tools in the workflow
        tool_defs = [
            tools._build_add_layer_def(
                tables=[table.name for table in tables],
                field_defs=field_defs
            ),
            tools._build_remove_layer_def(self.map_handler),
            tools._build_reset_def(),
            tools._build_run_analysis()
        ]

        # Call openai
        res = await self.client.responses.create(
            model=self.model,
            input=self.messages,
            tools=tool_defs
        )

        # Tool call loop:
        #   for each tool, extract args and make appropriate calls, keeping track of messages
        #   and proprely handling errors
        made_tool_calls = False
        for tool_call in res.output:
            # Validate that a tool was called
            self.messages.append(tool_call)
            if tool_call.type != "function_call":
                continue
            made_tool_calls = True

            # Extract info from call content
            kwargs = json.loads(tool_call.arguments)
            logger.info(f"Calling {tool_call.name} with kwargs: {kwargs}")

            # Special case for adding a layer to the map
            #   CQL filters must be build out before adding
            if tool_call.name == "add_map_layer":   
                # Extract filters from the kwargs
                # TODO: Clean up this logic to be less-convoluted
                filter_names = [
                    name
                    for name in kwargs.keys()
                    if name not in ['layer_id', 'color', 'style', 'table']
                ]
                filters = []
                for filter_name in filter_names:
                    filter_details = kwargs.pop(filter_name)
                    filters.append(HandlerFilter(
                        field=filter_name,
                        value=filter_details['value'],
                        op=filter_details['operator'],
                    ))
                table = self.registry[('table', kwargs['table'])][0]
                # Run the function with the new filter arg injected
                try:
                    kwargs['filters'] = filters
                    kwargs['table'] = table
                    self.tools['add_map_layer'](**kwargs)
                    # This tool has a custom response to handle how many parcels were selected
                    tool_response = f"{self.data_handler.filter_count(self.engine, f"{table.schema}.{table.name}", filters)} parcels found"
                except Exception as e:
                    logger.error(e)
                    tool_response = f"Tool call: {tool_call.name} failed, raised: {str(e)}"
            # Seperated to explicilty call asyncronhously
            elif tool_call.name == "run_analysis":
                try:
                    tool_response = await self.run_analysis(**kwargs)
                except Exception as e:
                    logger.error(e)
                    tool_response = f"Tool call: {tool_call.name} failed, raised: {str(e)}"
            else:
                try:
                    tool_response = self.tools[tool_call.name](**kwargs)
                except Exception as e:
                    logger.error(e)
                    tool_response = f"Tool call: {tool_call.name} failed, raised: {str(e)}"
            # Update all data
            logger.debug(f"Tool Response: {tool_response}")
            # dev message should be updated with latest status of the map
            self._update_dev_message(context=context)
            self.messages.append({
                "type": "function_call_output",
                "call_id": tool_call.call_id,
                "output": tool_response
            })
    
        # Recall openai if any tool calls were made for a user-friendly resposne
        if made_tool_calls:
            res = await self.client.responses.create(
                model=self.model,
                input=self.messages,
            )
        
        ai_message = res.output_text
        logger.debug(f"LLM reply: {ai_message}")
        self.messages.append({'role': 'assistant', 'content': ai_message})
        return ai_message
    

    async def run_analysis(self, query: str):
        """
        Runs an analysis given a user message. This is a more time consuming process than 'chat',
        as it forces the agent to *think* and plan steps, then executes sql to create tables for
        the analysis

        Args:
            - query(str): Text descibing what the analysis should accomplish
        """
        logger.info(f"Running analysis for query: {query}")
        # Setup the system message template
        system_message_template = Template(source=pathlib.Path("./geo_assistant/agent/system_message.j2").read_text())
        # Query for relevant fields
        field_defs = await self.field_store.query(query, k=15)
        field_defs = self.registry.verify_fields(field_defs)
        field_names = [field['name'] for field in field_defs]
        # Query registry for all tables that make up the set of fields
        tables = self.registry[('schema', Configuration.db_base_schema), ('fields', field_names)]
        logger.debug(f"Tables: {tables}")
        # Create a new Analysis Model with those fields as Enums (This forces the model to only
        #   use valid fields)
        DynGISModel = _GISAnalysis.build_model(
            step_types=[_AggregateStep, _MergeStep, _BufferStep, _FilterStep, _PlotlyMapLayerStep],
            fields=field_names,
            tables=[table.name for table in tables]
        )
        # Query for relative info
        context = await self.info_store.query(query, k=10)
        # Generate the system message
        system_message = system_message_template.render(
            field_definitions=field_defs,
            context_info=context,
            tables=tables
        )
        logger.debug(system_message)
        
        # Hit openai to generate a step-by-step plan for the analysis
        res: ParsedResponse[_GISAnalysis] = openai.Client(api_key=Configuration.openai_key).responses.parse(
            input=[
                {'role': 'system', 'content': system_message},
                {'role': 'user', 'content': query}
            ],
            model="o4-mini",
            reasoning={
                "effort":"high",
            },
            text_format=DynGISModel
        )
        analysis = res.output_parsed
        logger.debug(analysis.model_dump_json(indent=2))
        # Run through the steps, executing each query
        logger.debug(f"Steps: {[step.name for step in analysis.steps]}")

        try:
            # Execute and gather the report
            report = analysis.execute(self.engine)
            # Perform any actions required based on the report
            for item in report.items:
                if isinstance(item, TableCreated):
                    table = self.registry.register(
                        name=item.table,
                        engine=self.engine
                    )
                    table._postprocess(self.engine)
                elif isinstance(item, PlotlyMapLayerArguements):
                    table = self.registry[('table', item.source_table)][0]
                    self.map_handler._add_map_layer(
                        table=table,
                        layer_id=item.layer_id,
                        color=item.color
                    )
                else:
                    logger.warning(
                        f"Report item type {type(item)} handler not implemented"
                    )
        finally:
            # No matter what, drop all the tables but the last possible
            if len(analysis.output_tables) > 1:
                for table in analysis._final_tables:
                    logger.debug(f"Dropping {table}...")
                    table = self.registry[('table', table)][0]._drop(self.engine)
                
        return (
            f"GIS Analysis complete."
            f"Report description:"
            f"{report.model_dump_json(indent=2)}"
        )