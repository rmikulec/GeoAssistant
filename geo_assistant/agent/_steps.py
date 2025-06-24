"""
The file defines all `step` classes that can be used / interpreted by OpenAI to build out an analysis

_SQLSteps are intended to be any step that will execute a query on the database. It must have a
    corresponding jinja template file in `geo_assistant/agent/templates` that matches its `_type`
    to run. These steps are prefilled with an `output_table` that will be created upon execution.
    See other templates in folder for examples.

_ReportingStep is a placeholder class for now, intended on being expanded upon if any other types
    of reporting information are to be implemented. This could be something like extracting data
    to a csv, pdf, etc. As of right now there is one: _PlotlyMapLayer, which is contains data that
    can be used to add a new Vector Layer to a Plotly Map object.
"""

import uuid
from enum import Enum
from abc import ABC, abstractmethod
from typing import Type, Self, Literal, Optional, Union
from pydantic import BaseModel, Field, create_model
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, text

from geo_assistant.logging import get_logger

from geo_assistant.config import Configuration
from geo_assistant.agent._sql_exec import execute_template_sql
from geo_assistant.agent.report import PlotlyMapLayerArguements, TableCreated, SaveTable
from geo_assistant.agent._filter import SQLFilters, _FilterItem
from geo_assistant.agent._aggregator import SQLAggregators, _Aggregator

logger = get_logger(__name__)

"""
Helper classes for restricting data by declaring dynamic field placeholders, or literals
"""
# Placeholder type that will be replaced by a `Fields` enum upon class production
_Field = Type[str]
# All Geometry types that are compatible with PostGIS
GeometryType = Literal[
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
    "GeometryCollection",
    "Geometry"
]


class _SourceTable(BaseModel):
    """
    Source Table type that is used to restrict OpenAI from using one of two different types of
        source tables (either in a FROM statement or a JOIN statement)
    
    Args:
        - output_table_idx (Optional[int]): forces OpenAI to populate with an integer. As of
            6/24/2025, OpenAI does not support min / max values for json schemas, so this will have
            to rely on prompting. A model validator is then used to inject the table name to this
            field based on the index supplied
        - source_table (Optional[int]): A string value, that is replaced on model production, with
            a tables enum. This is to restrict OpenAI from generating invalid table names.
    """
    output_table_idx: Optional[int] = Field(description="If using the output of a previous step, supply the index here")
    source_table: Optional[str] = Field(description="The name of the source table to pull data from")
    source_schema: SkipJsonSchema[Optional[str]] = Field(default=None)


    def __str__(self):
        return f"{self.source_schema}.{self.source_table}"

    @classmethod
    def _build_model(cls, tables_enum: type[Enum]):
        """
        Private method called on model production to inject tables enum
        """
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            source_table=(tables_enum, Field(description="The name of the source table to pull data from"))
        )

"""
Step absract classes, to be extended in order to create valid steps
"""


class _GISAnalysisStep(BaseModel, ABC):
    """
    Base Analysis step holding core logic for building a step model. Every step will have a
        pregenerated id, and will require a name and reasoning for logging purposes.
    """
    id_: SkipJsonSchema[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="A descriptive name for the step")
    reasoning: str = Field(description="Description of what the step does, and why it is needed")
    
    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
        """
        Private method called on model production to inject fields and tables into all model_fields
            that are in the subclasses
        """

        # find all the _Field markers and replace them with a Fields enum
        dynamic_fields = {
            field_name: fields_enum
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == _Field
        }
        # Find all list[_Field] markers and replace them with a Fields Enum list-type
        dynamic_list_fields = {
            field_name: list[fields_enum]
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == list[_Field]
        }
        # Find all _SourceTable markers and run their _build_model private method
        table_fields = {
            field_name: field_info.annotation._build_model(tables_enum)
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == _SourceTable
        }
        
        # Create the new model, extending itself but replacing all marked fields with the newly
        #   generated ones.
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            **(dynamic_fields | dynamic_list_fields | table_fields)
        )
    

class _ReportingStep(_GISAnalysisStep, ABC):
    """
    Placeholder class to leave room open for future reporting steps
    """

    @abstractmethod
    def export(self):
        pass


class _SQLStep(_GISAnalysisStep, ABC):
    """
    Base class for sql steps, including logic for running the sql query. All subclasses **must**
    have a template in 'geo_assistant/agent/templates' with the same name as `_type`. Model fields
    will automatically be injected into that jinja template
    """
    _type: Literal["base"] = "base"
    output_table: str = Field(..., description="Name of table being created")

    def _get_geometry_type(
        self,
        engine: Engine,
        geometry_column: str = Configuration.geometry_column,
    ) -> None:
        """
        Scans the specified table for existing geometry subtypes, selects an
        appropriate Multi* typmod (or GeometryCollection), and normalizes the
        geometry column so that all rows share the same type and SRID.

        Args:
            engine (Engine): SQLAlchemy Engine connected to your PostGIS database
            geometry_column (str): Name of the geometry column to normalize
        """

        # Retrieve all table fields
        tables = []
        for name, f in self.__class__.model_fields.items():
            if issubclass(f.annotation, _SourceTable):
                tables.append(getattr(self, name))

        # Helper function to decide with geometry
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

        with engine.begin() as conn:
            # Gather distinct geometry types
            results = [
                conn.execute(
                    text(f'SELECT DISTINCT GeometryType({geometry_column}) FROM "{table.source_schema}"."{table.source_table}"')
                )
                for table in tables
            ]
            geom_types = {row[0] for result in results for row in result}
            # Choose the target typmod
            return choose_typmod(geom_types)


    def _execute(self, engine: Engine, schema: str) -> TableCreated:
        """
        Private method to execute the sql steps query on the PostGIS database.

        As of right now, it creates a new table for each step. Future iteration may look into using
        Views instead
        """

        # Get the geometry type for the new table
        geometry_type = self._get_geometry_type(engine)

        # Build out the args, excluding ones that are not needed
        exclude_args = ['_type', '_is_intermediate']
        other_args = self.model_dump(exclude=exclude_args)
        execute_template_sql(
            engine=engine,
            template_name=self._type,
            geometry_column=Configuration.geometry_column,
            srid=3857,
            gtype=geometry_type,
            schema=schema,
            **other_args
        )

        # Return a `TableCreated` reporting item
        return TableCreated(
            name=self.name,
            reason=self.reasoning,
            table_created=self.output_table,
        )
    


class _FilterStep(_SQLStep):
    """
    Filter step that runs a basic `WHERE` clause
    """
    _type: Literal['filter'] = "filter"
    select: list[_Field]
    source_table: _SourceTable
    filters: list[_FilterItem]

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
        """
        Needs a special build function to inject field enum into the subsequent filter classes.
        """
        cls = super()._build_step_model(fields_enum=fields_enum, tables_enum=tables_enum)
        dynamic_filters = [
            filter_._build_filter(fields_enum=fields_enum)
            for filter_ in SQLFilters
        ]
        filters_union = list[Union[tuple(dynamic_filters)]]
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            filters=(filters_union, ...)
        )
    
    def _execute(self, engine: Engine, schema: str):
        """
        Needs a special execute function to ensure that all filters are sql-safe
        """
        for f in self.filters:
            # single‐value filters
            if hasattr(f, "value") and isinstance(f.value, str):
                f.value = f"'{f.value}'"

            # list‐value filters (IN / NOT IN)
            if hasattr(f, "value_list"):
                f.value_list = [
                    f"'{v}'" if isinstance(v, str) else v
                    for v in f.value_list
                ]

            # BETWEEN filters
            if hasattr(f, "lower") and isinstance(f.lower, str):
                f.lower = f"'{f.lower}'"
            if hasattr(f, "upper") and isinstance(f.upper, str):
                f.upper = f"'{f.upper}'"

        return super()._execute(engine, schema)

class _MergeStep(_SQLStep):
    """
    SQL Step to run a basic `JOIN` clause
    """
    _type: SkipJsonSchema[Literal['merge']] = "merge"
    left_select: list[_Field]
    right_select: list[_Field]
    left_table: _SourceTable
    right_table: _SourceTable
    spatial_predicate: Literal['intersects','contains','within','dwithin'] = Field(
        ..., description="ST_<predicate> or DWithin"
    )
    output_geometry_type: GeometryType = Field(description="The geometry type after the spatial join. Choose carefull based on left, right table types as well as the spatial predicate")
    distance: Optional[float] = Field(
        None,
        description="Buffer distance (only for dwithin)"
    )
    keep_geometry: Literal['left','right'] = Field(
        'left',
        description="Which geometry column to keep in the output"
    )


class _AggregateStep(_SQLStep):
    """
    SQL Step to run a basic `GROUP BY` clause
    """
    _type: SkipJsonSchema[Literal['aggregate']] = "aggregate"
    select: list[_Field]
    source_table: _SourceTable
    aggregators: list[_Aggregator] = Field(..., description="List of ways to aggregate columns")
    spatial_aggregator: Optional[Literal[
    'COLLECT',      # ST_Collect
    'UNION',        # ST_Union
    'CENTROID',     # ST_Centroid
    'EXTENT',       # ST_Extent
    'ENVELOPE',     # ST_Envelope
    'CONVEXHULL'    # ST_ConvexHull
]] = Field(default=None, description="List of ways to aggregate geometries")
    group_by: list[_Field] = Field(..., description="List of columns to GROUP BY")
    output_table: str = Field(..., description="Name of the aggregated table")

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
        """
        Needs a special build function to inject fields enum into the Aggregator classes
        """
        cls = super()._build_step_model(fields_enum=fields_enum, tables_enum=tables_enum)
        dynamic_aggregators = [
            agg_._build_aggregator(fields_enum=fields_enum)
            for agg_ in SQLAggregators
        ]
        
        agg_union = list[Union[tuple(dynamic_aggregators)]]
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            aggregators=(agg_union, Field(..., description="List of ways to aggregate columns")),
        )


class _BufferStep(_SQLStep):
    """
    SQL Step to run a GIS Buffer analysis
    """
    _type: SkipJsonSchema[Literal['buffer']] = "buffer"
    source_table: _SourceTable
    buffer_distance: float = Field(..., description="Distance to buffer")
    buffer_unit: Literal['meters','kilometers'] = Field(
        'meters',
        description="Unit for the buffer distance. MUST be greater than 1. A buffer of 0 will result in an empty polygon"
    )


class _PlotlyMapLayerStep(_ReportingStep):
    """
    Reporting step to export data as a Plotly Map Layer
    """
    _type: SkipJsonSchema[Literal["addLayer"]] = "addLayer"
    source_table: _SourceTable
    layer_id: str = Field(description="The id of the new map layer")
    color: str = Field(description="Hex value of the color of the geometries")


    def export(self) -> PlotlyMapLayerArguements:
        return PlotlyMapLayerArguements(
            name=self.name,
            layer_id=self.layer_id,
            reason=self.reasoning,
            source_table=str(self.source_table),
            color=self.color
        )
    

class _SaveTable(_ReportingStep):
    _type: SkipJsonSchema[Literal["saveTable"]] = "saveTable"
    source_table: _SourceTable

    def export(self):
        schema, table = self.source_table.split('.')
        return SaveTable(
            table=table,
            schema=schema
        )
    
# List of default steps to be used if not specified else
DEFAULT_STEP_TYPES = [
    _AggregateStep,
    _FilterStep,
    _MergeStep,
    _BufferStep,
    _PlotlyMapLayerStep,
    _SaveTable
]