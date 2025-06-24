import uuid
from enum import Enum
from typing import Type, Self, Literal, Optional, Union, get_args
from pydantic import BaseModel, Field, create_model, model_validator
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, text

from geo_assistant.logging import get_logger

from geo_assistant.config import Configuration
from geo_assistant.agent._sql_exec import execute_template_sql
from geo_assistant.agent.report import GISReport, MapLayerCreated, TableCreated
from geo_assistant.agent._filter import SQLFilters, _FilterItem
from geo_assistant.agent._aggregator import SQLAggregators, _Aggregator

logger = get_logger(__name__)

DynamicField = Type[str]


def make_enum(*values: str) -> Type[Enum]:
    """
    Dynamically constructs an Enum subclass.

    - `name` is the Enum class name.
    - `values` are the allowed string values.
    
    Each member will be named as the upper-cased version of the value,
    and its `.value` will be the original string.
    """
    members = { val.upper(): val for val in values }
    return Enum("Fields", members)


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
    output_table_idx: Optional[int] = Field(description="If using the output of a previous step, supply the index here")
    source_table: Optional[str] = Field(description="The name of the source table to pull data from")

    @classmethod
    def _build_model(cls, tables_enum: type[Enum]):
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            source_table=(tables_enum, Field(description="The name of the source table to pull data from"))
        )


class _GISAnalysisStep(BaseModel):
    id_: SkipJsonSchema[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="A descriptive name for the step")
    reasoning: str = Field(description="Description of what the step does, and why it is needed")
    
    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
        """
        Return a new subclass of this model where every
        field annotated as DynamicField is replaced by Literal[options].
        """

        # find all the DynamicField markers and replace them
        dynamic_fields = {
            field_name: fields_enum
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == DynamicField
        }
        dynamic_list_fields = {
            field_name: list[fields_enum]
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == list[DynamicField]
        }
        table_fields = {
            field_name: field_info.annotation._build_model(tables_enum)
            for field_name, field_info in cls.model_fields.items()
            if field_info.annotation == _SourceTable
        }
        
        # give it a distinct name so Pydantic can differentiate
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            **(dynamic_fields | dynamic_list_fields | table_fields)
        )
    

class _ReportingStep(_GISAnalysisStep):
    
    def export(self):
        pass


class _SQLStep(_GISAnalysisStep):
    _type: Literal["base"] = "base"
    _is_intermediate: bool = False
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

        Parameters:
        - engine: SQLAlchemy Engine connected to your PostGIS database
        - table: Full table name, e.g. 'schema.table' or 'table'
        - geometry_column: Name of the geometry column to normalize
        - srid: Target SRID (default: 3857 for Web Mercator)
        """
        tables = []
        for name, f in self.__class__.model_fields.items():
            if issubclass(f.annotation, _SourceTable):
                tables.append(getattr(self, name))

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
            # 1) Gather distinct geometry types
            results = [
                conn.execute(
                    text(f"SELECT DISTINCT GeometryType({geometry_column}) FROM {table}")
                )
                for table in tables
            ]
            geom_types = {row[0] for result in results for row in result}
            # 2) Choose the target typmod
            return choose_typmod(geom_types)


    def _execute(self, engine: Engine, schema: str, output_tables: list[str]) -> TableCreated:
        geometry_type = self._get_geometry_type(engine)

        
        exclude_args = ['select', '_type', '_is_intermediate']
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

        return TableCreated(
            name=self.name,
            reason=self.reasoning,
            table=self.output_table,
            is_intermediate=True
        )

    def _drop(self, engine: Engine) -> None:
        """
        Drop a table if it exists.

        :param engine: SQLAlchemy Engine connected to your PostGIS database.
        :param table_name: Name of the table to drop (no schema).
        :param schema: Schema where the table lives (defaults to 'public').
        """
        execute_template_sql(
            engine=engine,
            template_name="drop",
            output_tables=[self.output_table]
        )

    


class _FilterStep(_SQLStep):
    _type: Literal['filter'] = "filter"
    select: list[DynamicField]
    source_table: _SourceTable
    filters: list[_FilterItem]

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
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
    
    def _execute(self, engine: Engine, schema: str, output_tables: list[str]):
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

        return super()._execute(engine, schema, output_tables)

class _MergeStep(_SQLStep):
    _type: SkipJsonSchema[Literal['merge']] = "merge"
    left_select: list[DynamicField]
    right_select: list[DynamicField]
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
    _type: SkipJsonSchema[Literal['aggregate']] = "aggregate"
    select: list[DynamicField]
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
    group_by: list[DynamicField] = Field(..., description="List of columns to GROUP BY")
    output_table: str = Field(..., description="Name of the aggregated table")

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
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
    _type: SkipJsonSchema[Literal['buffer']] = "buffer"
    source_table: _SourceTable
    buffer_distance: float = Field(..., description="Distance to buffer")
    buffer_unit: Literal['meters','kilometers'] = Field(
        'meters',
        description="Unit for the buffer distance. MUST be greater than 1. A buffer of 0 will result in an empty polygon"
    )


class _AddMapLayer(_ReportingStep):
    _type: SkipJsonSchema[Literal["addLayer"]] = "addLayer"
    source_table: _SourceTable
    layer_id: str = Field(description="The id of the new map layer")
    color: str = Field(description="Hex value of the color of the geometries")


    def export(self) -> MapLayerCreated:
        return MapLayerCreated(
            name=self.name,
            layer_id=self.layer_id,
            reason=self.reasoning,
            source_table=self.source_table,
            color=self.color
        )


class _GISAnalysis(BaseModel):
    name: str = Field(description="Snake case name of the analysis")
    steps: list[_GISAnalysisStep]
    #Private variables to not be exposed by pydantic, but used for running the analysis
    _tables_created: SkipJsonSchema[list[str]] = []

    @classmethod
    def build_model(cls, steps: list[Type[_SQLStep]], fields: list[str], tables: list[str]) -> Type["_GISAnalysis"]:
        """
        Returns a new GISAnalysis subclass where each of the step models
        (AggregateStep, MergeStep) has had its DynamicField replaced.
        """
        fields_enum = make_enum(*fields)
        tables_enum = make_enum(*tables)
        # generate dynamic versions of each SQLStep subclass
        dynamic_steps = [
            step_model._build_step_model(fields_enum=fields_enum, tables_enum=tables_enum)
            for step_model in steps
        ]
        # build Union[DynAgg, DynMerge]
        StepUnion = Union[tuple(dynamic_steps)]  # type: ignore[misc]

        # override only the 'steps' field
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            steps=(list[StepUnion], ...)
        )
    

    @property
    def output_tables(self) -> list[str]:
        """
        List of all the tables that are / will be created when executing analysis
        """
        return [
            step.output_table
            for step in self.steps
            if issubclass(step.__class__, _SQLStep)
        ]
    
    @property
    def sql_steps(self) -> list[_SQLStep]:
        return [
            step
            for step in self.steps
            if issubclass(step.__class__, _SQLStep)
        ]
    
    @property
    def reporting_steps(self) -> list[_ReportingStep]:
        return [
            step
            for step in self.steps
            if issubclass(step.__class__, _ReportingStep)
        ]

    @model_validator(mode="after")
    def _fill_in_source_tables(self):
        """
        Validator updates *any* field in *any* step that is a source table to have a string value,
            in the form of {schema}.{table}
        """
        for step in self.steps:
            for field, info in step.__class__.model_fields.items():
                if issubclass(info.annotation, _SourceTable):
                    value: _SourceTable = getattr(step, field)
                    if value.output_table_idx:
                        new_value = f"{self.name}.{self.output_tables[value.output_table_idx]}"
                    else:
                        new_value = f"{Configuration.db_base_schema}.{value.source_table.value}"
                    setattr(step, field, new_value)
        return self

    def execute(self, engine: Engine) -> GISReport:
        with engine.begin() as conn:
            sql = text(
                (
                    f"CREATE SCHEMA IF NOT EXISTS {self.name} AUTHORIZATION pg_database_owner;"
                    f"GRANT USAGE ON SCHEMA {self.name} TO pg_database_owner;"
                )
            )
            conn.execute(sql)

        items = []

        for step in self.steps:
            logger.info(f"Running {step.name}")
            logger.debug(step.reasoning)

            if isinstance(step, _SQLStep):
                items.append(step._execute(engine, self.name, self.output_tables))
            elif isinstance(step, _AddMapLayer):
                items.append(step.export())
            
        return GISReport(
            items=items
        )