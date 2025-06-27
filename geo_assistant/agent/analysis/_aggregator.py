"""
File contains logic / classes to build out Aggregator classes for OpenAI structured outputs

This allows to creation of a json schema, complete with enums, for different types of group by
    operations
"""

from abc import ABC
from typing import Literal, Optional, Union
from pydantic import Field, ConfigDict

from geo_assistant.agent.analysis._select import _Column

# all supported aggregation functions
AggregatorOperator = Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']

class _AggregateColumn(_Column):
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


"""
Aggregator classes each for a different type of GROUP BY operation. To add more, extend the base
    class and add to the list at the bottom
"""


class _Count(_AggregateColumn):
    operator: Literal['COUNT'] = "COUNT"
    column:   Union[str, Literal['*']]   = Field(
        '*',
        description="Column to count (or '*' for all rows)"
    )
    distinct: Optional[bool]                 = Field(
        False,
        description="Whether to count only distinct values"
    )


class _Sum(_AggregateColumn):
    operator: Literal['SUM'] = "SUM"
    column:   str                   = Field(..., description="Column to sum")


class _Avg(_AggregateColumn):
    operator: Literal['AVG'] = "AVG"
    column:   str                   = Field(..., description="Column to average")


class _Min(_AggregateColumn):
    operator: Literal['MIN'] = "MIN"
    column:   str                   = Field(..., description="Column to take minimum of")


class _Max(_AggregateColumn):
    operator: Literal['MAX'] = "MAX"
    column:   str                   = Field(..., description="Column to take maximum of")


# The union type you’ll actually use:
SQLAggregators: list[_AggregateColumn] = [
    _Count,
    _Sum,
    _Avg,
    _Min,
    _Max,
]
