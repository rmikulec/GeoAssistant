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
from typing import Type, Self, Literal, Optional, Union, get_args
from pydantic import BaseModel, Field, create_model, computed_field
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, text

from geo_assistant.logging import get_logger

from geo_assistant.config import Configuration
from geo_assistant._sql._sql_exec import execute_template_sql
from geo_assistant.agent.analysis.report import PlotlyMapLayerArguements, TableCreated, SaveTable
from geo_assistant.agent.analysis._select import _Column
from geo_assistant.agent.analysis._filter import _WhereColumn
from geo_assistant.agent.analysis._aggregator import _AggregateColumn

logger = get_logger(__name__)

"""
Helper classes for restricting data by declaring dynamic field placeholders, or literals
"""
# Placeholder model to mark a field as a Table for later resolution
class _Table(BaseModel):
    pass

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


"""
Step absract classes, to be extended in order to create valid steps
"""


class _GISAnalysisStep(BaseModel, ABC):
    """
    Base Analysis step holding core logic for building a step model. Every step will have a
        pregenerated id, and will require a name and reasoning for logging purposes.
    """
    id_: SkipJsonSchema[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    table_: SkipJsonSchema[str] = None
    to_destroy_: SkipJsonSchema[bool] = None
    
    @classmethod
    def _build_step_model(cls, columns_enum: type[Enum], tables_enum: type[Enum]) -> Type[Self]:
        """
        Private method to resolve special fields, injecting them with the proper enums
        Its important to use the same enum instance for these, because it will result
        in the json schema used a shared ref across all models, reducing token size
        """
        # Any subclasses of column
        resolved_fields = {}
        for field, info in cls.model_fields.items():
            anno_args = get_args(info.annotation)

            if len(anno_args) == 0:
                if info.annotation == _Column:
                    resolved_fields[field] = info.annotation._build(columns_enum)
                elif issubclass(info.annotation, _Column):
                    resolved_fields[field] = info.annotation._build(columns_enum)
                elif info.annotation == _Table:
                    resolved_fields[field] = tables_enum
            else:
                list_annotation = anno_args[0]
                if list_annotation == _Column:
                    resolved_fields[field] = list[list_annotation._build(columns_enum)]
                elif issubclass(list_annotation, _Column):
                    resolved_fields[field] = list_annotation._build(columns_enum)
                elif list_annotation == _Table:
                    resolved_fields[field] = list[tables_enum]

        # Create the new model, extending itself but replacing all marked fields with the newly
        #   generated ones.
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            **resolved_fields
        )
    

class _ReportingStep(_GISAnalysisStep, ABC):
    """
    Placeholder class to leave room open for future reporting steps
    """

    @abstractmethod
    def export(self):
        pass


class _SQLStep(_GISAnalysisStep, ABC):
    _output = []
    """
    Base class for sql steps, including logic for running the sql query. All subclasses **must**
    have a template in 'geo_assistant/agent/templates' with the same name as `_type`. Model fields
    will automatically be injected into that jinja template
    """
    step_type: Literal["base"] = "base"

    @computed_field
    def columns(self) -> list[str]:
        cols = []
        for field in self._output:
            value = getattr(self, field)
            if isinstance(value, list):
                cols.extend([v.column.value for v in value])
            else:
                cols.append(value.column.value)
        return cols

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
            if issubclass(f.annotation, _Table):
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


    def _execute(self, engine: Engine, schema: str, gtype: str = None) -> TableCreated:
        """
        Private method to execute the sql steps query on the PostGIS database.

        As of right now, it creates a new table for each step. Future iteration may look into using
        Views instead
        """

        if not gtype:
            # Get the geometry type for the new table
            gtype = self._get_geometry_type(engine)

        # Build out the args, excluding ones that are not needed
        exclude_args = ['_type', '_is_intermediate']
        other_args = self.model_dump(exclude=exclude_args)
        execute_template_sql(
            engine=engine,
            template_name=self.step_type,
            geometry_column=Configuration.geometry_column,
            srid=3857,
            gtype=gtype,
            schema=schema,
            **other_args
        )

        execute_template_sql(
            engine=engine,
            template_name="postprocess",
            schema=schema,
            table=self.output_table
        )

        # Return a `TableCreated` reporting item
        return TableCreated(
            name=self.name,
            reason=self.reasoning,
            table=self.output_table,
            columns=self.columns
        )
    


class _FilterStep(_SQLStep):
    _output = ["select"]
    """
    Filter step that runs a basic `WHERE` clause
    """
    step_type: Literal['filter'] = "filter"
    select: list[_Column]
    from_table: _Table
    where_clause: list[_WhereColumn]
    order_by: list[_Column]
    order_desc: bool
    limit: int
        
    
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
    _output = ["left_select", "right_select"]
    """
    SQL Step to run a basic `JOIN` clause
    """
    step_type: Literal['merge'] = "merge"
    left_select: list[_Column]
    right_select: list[_Column]
    from_left_table: _Table
    join_right_table: _Table
    spatial_aggregator: Optional[Literal[
        'COLLECT',      # ST_Collect
        'UNION',        # ST_Union
        'CENTROID',     # ST_Centroid
        'EXTENT',       # ST_Extent
        'ENVELOPE',     # ST_Envelope
        'CONVEXHULL',   # ST_ConvexHull
        'CONCAVEHULL'
    ]] = Field(default=None, description="List of ways to aggregate geometries on merge")
    spatial_predicate: Literal['intersects','contains','within','dwithin'] = Field(
        ..., description="ST_<predicate> or DWithin"
    )
    distance: Optional[float] = Field(
        None,
        description="Buffer distance (only for dwithin)"
    )

class _AggregateStep(_SQLStep):
    _output = ["select", "group_by"]
    """
    SQL Step to run a basic `GROUP BY` clause
    """
    step_type: Literal['aggregate'] = "aggregate"
    select: list[_AggregateColumn]
    from_table: _Table
    spatial_aggregator: Optional[Literal[
        'COLLECT',      # ST_Collect
        'UNION',        # ST_Union
        'CENTROID',     # ST_Centroid
        'EXTENT',       # ST_Extent
        'ENVELOPE',     # ST_Envelope
        'CONVEXHULL',    # ST_ConvexHull
        'CONCAVEHULL'
    ]] = Field(default=None, description="List of ways to aggregate geometries")
    group_by: list[_Column] = Field(..., description="List of columns to GROUP BY")



class _BufferStep(_SQLStep):
    _output = []
    """
    SQL Step to run a GIS Buffer analysis
    """
    step_type: Literal['buffer'] = "buffer"
    from_table: _Table
    buffer_distance: float = Field(..., description="Distance to buffer")
    buffer_unit: Literal['meters','kilometers'] = Field(
        'meters',
        description="Unit for the buffer distance. MUST be greater than 1. A buffer of 0 will result in an empty polygon"
    )


class _PlotlyMapLayerStep(_ReportingStep):
    """
    Reporting step to export data as a Plotly Map Layer
    """
    step_type: Literal["addLayer"] = "addLayer"
    source_table: _Table
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
    step_type: Literal["saveTable"] = "saveTable"
    source_table: _Table

    def export(self):
        schema, table = self.source_table.split('.')
        return SaveTable(
            table=table,
            schema=schema
        )
    
# List of default steps to be used if not specified else
STEP_TYPES: dict[str, _GISAnalysisStep] = {
    "Aggregate": _AggregateStep,
    "Filter": _FilterStep,
    "Merge": _MergeStep,
    "Buffer": _BufferStep,
    "Plot": _PlotlyMapLayerStep,
    "Save": _SaveTable
}