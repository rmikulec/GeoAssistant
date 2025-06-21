import uuid
from enum import Enum
from typing import Type, Self, Literal, Optional, Union, get_args
from pydantic import BaseModel, Field, create_model
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, text

from geo_assistant.config import Configuration
from geo_assistant.agent._sql_exec import execute_template_sql
from geo_assistant.agent._filter import SQLFilters, _FilterItem
from geo_assistant.agent._aggregator import SQLAggregators, _Aggregator

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


class _GISAnalysisStep(BaseModel):
    id_: SkipJsonSchema[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="A descriptive name for the step")
    reasoning: str = Field(description="Description of what the step does, and why it is needed")
    
    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum]) -> Type[Self]:
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
        
        # give it a distinct name so Pydantic can differentiate
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            **(dynamic_fields | dynamic_list_fields)
        )


class _SQLStep(_GISAnalysisStep):
    _type: Literal["base"] = "base"
    _is_intermediate: bool = False
    select: list[DynamicField]
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
            if getattr(f, "json_schema_extra"):
                if f.json_schema_extra.get("is_table", False):
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

    def _create_spatial_index(self, engine: Engine, table: str):
        with engine.begin() as conn:
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS "public".{table}_geometry_gist_idx ON "public"."{table}" USING GIST (geometry);'

                )
            )
            print("analyze")
            conn.execute(
                text(f'ANALYZE "{table}"')
            )

    def _execute(self, engine: Engine):
        geometry_type = self._get_geometry_type(engine)

        execute_template_sql(
            engine=engine,
            template_name=self._type,
            geometry_column=Configuration.geometry_column,
            srid=3857,
            gtype=geometry_type,
            select_columns=self.select,
            **self.model_dump(exclude=['select', '_type', '_is_intermediate'])
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
    source_table: str = Field(..., is_table=True)
    filters: list[_FilterItem]

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum]) -> Type[Self]:
        cls = super()._build_step_model(fields_enum=fields_enum)
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
    
    def _execute(self, engine: Engine = None):
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

        return super()._execute(engine)

class _MergeStep(_SQLStep):
    _type: SkipJsonSchema[Literal['merge']] = "merge"
    left_table: str = Field(..., is_table=True)
    right_table: str = Field(..., is_table=True)
    spatial_predicate: Literal['intersects','contains','within','dwithin'] = Field(
        ..., description="ST_<predicate> or DWithin"
    )
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
    source_table: str = Field(..., is_table=True)
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
    def _build_step_model(cls, fields_enum: type[Enum]) -> Type[Self]:
        cls = super()._build_step_model(fields_enum=fields_enum)
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
    source_table: str = Field(..., is_table=True)
    buffer_distance: float = Field(..., description="Distance to buffer")
    buffer_unit: Literal['meters','kilometers'] = Field(
        'meters',
        description="Unit for the buffer distance. MUST be greater than 1. A buffer of 0 will result in an empty polygon"
    )


class _AddMapLayer(_GISAnalysisStep):
    _type: SkipJsonSchema[Literal["addLayer"]] = "addLayer"
    source_table: str
    layer_id: str = Field(description="The id of the new map layer")
    color: str = Field(description="Hex value of the color of the geometries")
    filters: list[_FilterStep]

    @classmethod
    def _build_step_model(cls, fields_enum: type[Enum]) -> Type[Self]:
        cls = super()._build_step_model(fields_enum=fields_enum)
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


class _GISAnalysis(BaseModel):

    @classmethod
    def build_model(cls, steps: list[Type[_SQLStep]], fields: list[str]) -> Type["_GISAnalysis"]:
        """
        Returns a new GISAnalysis subclass where each of the step models
        (AggregateStep, MergeStep) has had its DynamicField replaced.
        """
        fields_enum = make_enum(*fields)
        # generate dynamic versions of each SQLStep subclass
        dynamic_steps = [
            step_model._build_step_model(fields_enum=fields_enum)
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
    