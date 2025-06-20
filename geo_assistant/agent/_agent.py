from dotenv import load_dotenv
load_dotenv()

import logging
import pathlib
import openai
import json

from openai.types.responses import ParsedResponse
from collections import defaultdict
from typing import Callable, Any
from jinja2 import Template
from sqlalchemy import Engine, text

from geo_assistant.handlers import MapHandler, DataHandler, GeoFilter
from geo_assistant.doc_stores import FieldDefinitionStore, SupplementalInfoStore
from geo_assistant import tools
from geo_assistant.config import Configuration

from geo_assistant.agent._steps import _GISAnalysis, _AggregateStep, _FilterStep, _MergeStep, _BufferStep
from geo_assistant.agent._sql_exec import execute_template_sql


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
        supplement_info: str = None,
        use_smart_search: bool = True
    ):
        self.model = model
        self.engine = engine
        self.use_smart_search = use_smart_search
        self.supplement_info = supplement_info
        self.map_handler = map_handler
        self.data_handler = data_handler
        self.client = openai.AsyncOpenAI(api_key=pathlib.Path("./openai.key").read_text())

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
                    supplement_information=supplement_info
                )    
            }
        ]

        self.tools: dict[str, Callable[..., Any]] = {
            "add_map_layer":    self.map_handler._add_map_layer,
            "remove_map_layer": self.map_handler._remove_map_layer,
            "reset_map":        self.map_handler._reset_map,
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
                supplement_information=self.supplement_info
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


        tool_defs = [
            tools._build_add_layer_def(field_defs),
            tools._build_remove_layer_def(self.map_handler),
            tools._build_reset_def()
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

            print(f"Calling {tool_call.name} with filters: {kwargs}")


            if tool_call.name == "add_map_layer":   
                # Extract filters from the kwargs
                filter_names = [
                    name
                    for name in kwargs.keys()
                    if name not in ['layer_id', 'color', 'style']
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
    

    def _verify_fields(self, field_results: list[dict]):
        updated_results = []

        for metadata in self.map_handler._tables_meta.values():
            for property in metadata['properties']:
                for field_result in field_results:
                    if property['name'].lower() == field_result['name'].lower():
                        temp = field_result.copy()
                        temp.pop('name')
                        updated_results.append(
                            {
                                "name": property['name'],
                                **field_result
                            }
                        )

        return updated_results
    
    def _normalize_geometry(
        self,
        table: str,
        geometry_column: str = Configuration.geometry_column,
        srid: int = Configuration.srid
    ) -> None:
        """
        Scans the specified table for existing geometry subtypes, selects an
        appropriate Multi* typmod (or GeometryCollection), and normalizes the
        geometry column so that all rows share the same type and SRID.

        Parameters:
        - engine: SQLAlchemy Engine connected to your PostGIS database
        - table: Full table name, e.g. 'schema.table' or 'table'
        - geometry_column: Name of the geometry column to normalize
        - srid: Target SRID (default: 3857 for Web Mercator)
        """
        def choose_typmod(types: set[str]) -> str:
            poly = {'POLYGON', 'MULTIPOLYGON'}
            line = {'LINESTRING', 'MULTILINESTRING'}
            point = {'POINT', 'MULTIPOINT'}
            if types <= poly:
                return 'MultiPolygon'
            if types <= line:
                return 'MultiLineString'
            if types <= point:
                return 'MultiPoint'
            return 'GeometryCollection'

        with self.engine.begin() as conn:
            # 1) Gather distinct geometry types
            result = conn.execute(
                text(f"SELECT DISTINCT GeometryType({geometry_column}) FROM {table}")
            )
            geom_types = {row[0] for row in result}
            # 2) Choose the target typmod
            typmod = choose_typmod(geom_types)
            print(typmod)

            execute_template_sql(
                engine=self.engine,
                template_name="normalize",
                table=table,
                geometry_column=geometry_column,
                typmod=typmod,
                srid=srid
            )


    def _postprocess_table(
        self,
        table: str,
        geometry_column: str = Configuration.geometry_column,
        srid: int = Configuration.srid
    ):
        """
        After CTAS, normalize the geom column, register it,
        then grant/select, add GIST index, and analyze.
        """
        qualified = f'"public"."{table}"'

        # 1) normalize + register
        #print("Normalizing")
        #self._normalize_geometry(qualified, geometry_column, srid)

        # 2) grant, index, analyze
        with self.engine.begin() as conn:
            print("grant to public")
            conn.execute(
                text(f"GRANT SELECT ON {qualified} TO public")
            )
            print("creating index")
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {table}_geometry_gist_idx ON "{table}" USING GIST (geometry);'

                )
            )
            print("analyze")
            conn.execute(
                text(f'ANALYZE "{table}"')
            )


    async def run_analysis(self, user_message: str, _debug_path: str = None):
        """
        Runs an analysis given a user message. This is a more time consuming process than 'chat',
        as it forces the agent to *think* and plan steps, then executes sql to create tables for
        the analysis

        Args:
            - user_message(str): The question or statement from the user detailing the analysis
        """
        if _debug_path:
            fields = [
                "BldgFront", "BldgDepth", "ZoneDist3", "SUB_1"
            ]
            DynGISModel = _GISAnalysis.build_model(
                steps=[_AggregateStep, _MergeStep, _BufferStep, _FilterStep],
                fields=fields
            )
            analysis_steps = DynGISModel.model_validate(
                json.load(open(_debug_path, "r"))
            ).steps
        else:
            # Setup the system message template
            system_message_template = Template(source=pathlib.Path("./geo_assistant/agent/system_message.j2").read_text())
            # Query for relevant fields
            field_results = await self.field_store.query(user_message, k=15)
            fields = self._verify_fields(field_results)
            # Create a new Analysis Model with those fields as Enums (This forces the model to only
            #   use valid fields)
            DynGISModel = _GISAnalysis.build_model(
                steps=[_AggregateStep, _MergeStep, _BufferStep, _FilterStep],
                fields=[field['name'] for field in fields]
            )
            # Query for relative info
            context = await self.info_store.query(user_message, k=10)
            # Generate the system message
            system_message = system_message_template.render(
                field_definitions=fields,
                context_info=context,
                tables=set([field['table'] for field in fields])
            )
            print(system_message)

            # Hit openai to generate a step-by-step plan for the analysis
            res: ParsedResponse[_GISAnalysis] = openai.Client(api_key=Configuration.openai_key).responses.parse(
                input=[
                    {'role': 'system', 'content': system_message},
                    {'role': 'user', 'content': user_message}
                ],
                model="o4-mini",
                reasoning={
                    "effort":"high",
                },
                text_format=DynGISModel
            )
            analysis_steps = res.output_parsed.steps
            print(res.output_parsed.model_dump_json(indent=2))
        # Run through the steps, executing each query
        print(f"Steps: {[step.name for step in analysis_steps]}")

        tables_created = []

        try:
            for step in analysis_steps:
                print(f"Running {step.name}")
                print(step.reasoning)
                step._execute(self.engine)
                print("postprocessing...")
                self._postprocess_table(step.output_table)
                tables_created.append(step.output_table)
        except Exception as e:
            raise e
        finally:
            # No matter what, drop all the tables but the last possible
            if len(tables_created) > 1:
                tables_to_drop = tables_created[:(len(analysis_steps)-1)]
                print(f"Dropping: {tables_to_drop}")
                execute_template_sql(
                    engine=self.engine,
                    template_name="drop",
                    output_tables=tables_to_drop
            )