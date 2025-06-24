from typing import Type, Union
from enum import Enum
from pydantic import BaseModel, Field, create_model, model_validator
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, text

from geo_assistant.config import Configuration
from geo_assistant.logging import get_logger
from geo_assistant.agent.report import GISReport
from geo_assistant.agent._steps import _GISAnalysisStep, _SQLStep, _ReportingStep, _SourceTable, _AddMapLayer
from geo_assistant.agent._exceptions import AnalysisSQLStepFailed


logger = get_logger(__name__)


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
            logger.info(f"Running {step.name}: {step.reasoning}")

            if isinstance(step, _SQLStep):
                try:
                    items.append(step._execute(engine, self.name, self.output_tables))
                except Exception as e:
                    raise AnalysisSQLStepFailed(
                        analysis_name=self.name,
                        step=step,
                        exception=e
                    )
            elif isinstance(step, _AddMapLayer):
                items.append(step.export())
            
        return GISReport(
            items=items
        )