from dotenv import load_dotenv
load_dotenv()

import logging
import pathlib
import openai
import json

from openai.types.responses import ParsedResponse
from typing import Callable, Any
from jinja2 import Template
from sqlalchemy import Engine, text

from geo_assistant.handlers import MapHandler, DataHandler, GeoFilter
from geo_assistant.agent.report import TableCreated, MapLayerCreated
from geo_assistant.table_registry import TableRegistry
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore
from geo_assistant import tools
from geo_assistant.config import Configuration

from geo_assistant.agent._steps import _GISAnalysis, _AggregateStep, _FilterStep, _MergeStep, _BufferStep, _AddMapLayer


logger = logging.getLogger(__name__)

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

ANALYSIS_PERFORMANCE_GUIDELINES = """
- Break queries into short, sequential steps using temporary tables
- Filter early using indexed columns or bounding boxes
- Select only the columns required for the next step
- Avoid expensive spatial joins when a bounding box will do
- Drop intermediate tables once they are no longer needed
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
        engine: Engine,
        map_handler: MapHandler,
        data_handler: DataHandler,
        field_store: FieldDefinitionStore = None,
        info_store: SupplementalInfoStore = None,
        model: str = Configuration.inference_model,
        use_smart_search: bool = True
    ):
        self.model: str = model
        self.engine: Engine = engine
        self.use_smart_search: bool = use_smart_search
        self.map_handler: MapHandler = map_handler
        self.data_handler: DataHandler = data_handler
        self.client: openai.AsyncOpenAI = openai.AsyncOpenAI(api_key=pathlib.Path("./openai.key").read_text())
        self.registry: TableRegistry = TableRegistry.load_from_tileserv(self.engine)

        if field_store is None:
            self.field_store = FieldDefinitionStore(version=Configuration.field_def_store_version)
        else:
            self.field_store = field_store
        
        if info_store is None:
            self.info_store = SupplementalInfoStore(version=Configuration.info_store_version)
        else:
            self.info_store = info_store

        self.messages = [
            {'role': 'developer', 'content': GEO_AGENT_SYSTEM_MESSAGE.format(
                    map_status="Graph not updated yet.",
                    supplement_information=""
                )    
            }
        ]

        self.tools: dict[str, Callable[..., Any]] = {
            "add_map_layer":    self.map_handler._add_map_layer,
            "remove_map_layer": self.map_handler._remove_map_layer,
            "reset_map":        self.map_handler._reset_map,
            "run_analysis":     self.run_analysis
        }


    @property
    def conversation(self):
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
    

    def _update_dev_message(self):
        self.messages[0] = {'role': 'developer', 'content': GEO_AGENT_SYSTEM_MESSAGE.format(
                map_status=json.dumps(self.map_handler.status,indent=2),
                supplement_information=""
            )    
        }
    

    async def _get_field_defs(self, message: str, context: str = None, k: int = 5):
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
            field_defs(list[dict]): Field Definitions that can be used to filter the GeoDataframe
                These can be obtained by querying the "FieldDefinitionStore"
        Returns:
            str: The message generated by the LLM to return to the user
        """
        self.messages.append(
            {'role': 'user', 'content': user_message}
        )

        context_search = await self.info_store.query(user_message, k=3)
        context = "\n".join([result['markdown'] for result in context_search])
        field_defs = await self._get_field_defs(user_message, context)
        tables = list(self.map_handler._tileserv_index.keys())

        tool_defs = [
            tools._build_add_layer_def(
                tables=tables,
                field_defs=field_defs
            ),
            tools._build_remove_layer_def(self.map_handler),
            tools._build_reset_def(),
            tools._build_run_analysis()
        ]

        res = await self.client.responses.create(
            model=self.model,
            input=self.messages,
            tools=tool_defs
        )

        made_tool_calls = False
        for tool_call in res.output:
            self.messages.append(tool_call)
            if tool_call.type != "function_call":
                continue

            made_tool_calls = True
            kwargs = json.loads(tool_call.arguments)

            print(f"Calling {tool_call.name} with kwargs: {kwargs}")


            if tool_call.name == "add_map_layer":   
                # Extract filters from the kwargs
                filter_names = [
                    name
                    for name in kwargs.keys()
                    if name not in ['layer_id', 'color', 'style', 'table']
                ]
                filters = []
                for filter_name in filter_names:
                    filter_details = kwargs.pop(filter_name)
                    filters.append(GeoFilter(
                        field=filter_name,
                        value=filter_details['value'],
                        op=filter_details['operator'],
                        table="public."+self.field_store.get_docs_by_kv(key='name', value=filter_name)[0]['table']
                    ))
            
                # Run the function with the new filter arg injected
                try:
                    kwargs['filters'] = filters
                    self.tools['add_map_layer'](**kwargs)
                    # This tool has a custom response to handle how many parcels were selected
                    tool_response = f"{self.data_handler.filter_count(filters)} parcels found"
                except Exception as e:
                    tool_response = f"Tool call: {tool_call.name} failed, raised: {str(e)}"
            elif tool_call.name == "run_analysis":
                tool_response = await self.run_analysis(**kwargs)
            else:
                try:
                    tool_response = self.tools[tool_call.name](**kwargs)
                except Exception as e:
                    tool_response = f"Tool call: {tool_call.name} failed, raised: {str(e)}"
            print(f"Tool Response: {tool_response}")
            self._update_dev_message()
            self.messages.append({
                "type": "function_call_output",
                "call_id": tool_call.call_id,
                "output": tool_response
            })
    
        if made_tool_calls:
            res = await self.client.responses.create(
                model=self.model,
                input=self.messages,
            )
        
        ai_message = res.output_text
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
        # Setup the system message template
        system_message_template = Template(source=pathlib.Path("./geo_assistant/agent/system_message.j2").read_text())
        # Query for relevant fields
        fields = await self.field_store.query(query, k=15)
        fields = self.registry.verify_fields(fields)
        field_names = [field['name'] for field in fields]
        # Query registry for all tables that make up the set of fields
        tables = self.registry[('schema', Configuration.db_base_schema), ('fields', field_names)]
        print(tables)
        # Create a new Analysis Model with those fields as Enums (This forces the model to only
        #   use valid fields)
        DynGISModel = _GISAnalysis.build_model(
            steps=[_AggregateStep, _MergeStep, _BufferStep, _FilterStep, _AddMapLayer],
            fields=field_names,
            tables=[table.name for table in tables]
        )
        # Query for relative info
        context = await self.info_store.query(query, k=10)
        # Generate the system message
        system_message = system_message_template.render(
            field_definitions=fields,
            context_info=context,
            tables=tables,
            performance_guidelines=ANALYSIS_PERFORMANCE_GUIDELINES,
            analysis_goal=query,
            map_status=json.dumps(self.map_handler.status, indent=2)
        )
        print(system_message)
        
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
        print(analysis.model_dump_json(indent=2))
        # Run through the steps, executing each query
        print(f"Steps: {[step.name for step in analysis.steps]}")

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
                elif isinstance(item, MapLayerCreated):
                    self.map_handler._add_map_layer(
                        table=item.source_table,
                        layer_id=item.layer_id,
                        color=item.color
                    )
                else:
                    print(f"Report item type {type(item)} handler not implemented")
        except Exception as e:
            raise e
        finally:
            # No matter what, drop all the tables but the last possible
            if len(analysis.output_tables) > 1:
                tables_to_drop = analysis.output_tables[:(len(analysis._tables_created)-1)]
                print(f"Dropping: {tables_to_drop}")
                """
                execute_template_sql(
                    engine=self.engine,
                    template_name="drop",
                    output_tables=tables_to_drop
                )
                """
                
        return (
            f"GIS Analysis complete."
            f"Report description:"
            f"{report.model_dump_json(indent=2)}"
        )