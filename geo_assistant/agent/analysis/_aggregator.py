"""
File contains logic / classes to build out Aggregator classes for OpenAI structured outputs

This allows to creation of a json schema, complete with enums, for different types of group by
    operations
"""

from abc import ABC
from typing import Literal, Optional, Union
from pydantic import BaseModel, Field, ConfigDict, create_model

# all supported aggregation functions
AggregatorOperator = Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']

class _Aggregator(BaseModel, ABC):
    """
    Base Aggregator class, requiring an `operator` for each subclass, and providing an `alias`
        field for each subclass

    Base class also has a private build method to inject fields into a `column` model field
    """
    operator: AggregatorOperator = Field(..., description="Aggregation function")
    alias: Optional[str]          = Field(
        None,
        description="Optional alias for the aggregated output"
    )

    # use operator as discriminator
    model_config = ConfigDict(discriminator='operator')

    @classmethod
    def _build_aggregator(cls, fields_enum):
        """
        Private method to inject Fields Enum into a `column` model field
        """
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            column=(fields_enum, ...)
        )

"""
Aggregator classes each for a different type of GROUP BY operation. To add more, extend the base
    class and add to the list at the bottom
"""


class _CountAggregator(_Aggregator):
    operator: Literal['COUNT']
    column:   Union[str, Literal['*']]   = Field(
        '*',
        description="Column to count (or '*' for all rows)"
    )
    distinct: Optional[bool]                 = Field(
        False,
        description="Whether to count only distinct values"
    )


class _SumAggregator(_Aggregator):
    operator: Literal['SUM']
    column:   str                   = Field(..., description="Column to sum")


class _AvgAggregator(_Aggregator):
    operator: Literal['AVG']
    column:   str                   = Field(..., description="Column to average")


class _MinAggregator(_Aggregator):
    operator: Literal['MIN']
    column:   str                   = Field(..., description="Column to take minimum of")


class _MaxAggregator(_Aggregator):
    operator: Literal['MAX']
    column:   str                   = Field(..., description="Column to take maximum of")


# The union type you’ll actually use:
SQLAggregators: list[_Aggregator] = [
    _CountAggregator,
    _SumAggregator,
    _AvgAggregator,
    _MinAggregator,
    _MaxAggregator,
]
