"""
File contains logic / classes to build out Filter classes for OpenAI structured outputs

This allows to creation of a json schema, complete with enums, for different types of WHERE
    operations
"""

from abc import ABC
from typing import Union, List
from typing_extensions import Literal
from pydantic import BaseModel, ConfigDict, Field, create_model

from geo_assistant.agent.analysis._select import _Column

# literal of all operators
Operator = Literal[
    '=', '!=', '>', '<', '>=', '<=', 
    'IN', 'NOT IN', 
    'BETWEEN', 
    'LIKE', 'ILIKE', 
    'IS NULL', 'IS NOT NULL'
]


class _WhereColumn(_Column):
    """
    Base Filter class, requiring an `operator` for each subclass, and providing an `alias`
        field for each subclass

    Base class also has a private build method to inject fields into a `column` model field
    """
    operator: Operator = Field(..., description="Comparison operator")
    column: str = Field(..., description="Column name to filter")

    # enable discriminated union on `operator`
    model_config = ConfigDict(discriminator='operator')

"""
Filter classes each for a different type of WHERE operation. To add more, extend the base
    class and add to the list at the bottom
"""


class _ValueWhere(_WhereColumn):
    operator: Literal['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE']
    value: Union[str, int, float] = Field(
        ..., description="Single value for comparisons"
    )


class _ListWhere(_WhereColumn):
    operator: Literal['IN', 'NOT IN']
    value_list: List[Union[str, int, float]] = Field(
        ..., description="List of values for IN / NOT IN"
    )


class _BetweenWhere(_WhereColumn):
    operator: Literal['BETWEEN']
    lower: Union[str, int, float]
    upper: Union[str, int, float]


class _NullWhere(_WhereColumn):
    operator: Literal['IS NULL', 'IS NOT NULL']


# List of allowed filter types
SQLFilters: list[type[_WhereColumn]] = [
    _ValueWhere,
    _ListWhere,
    _BetweenWhere,
    _NullWhere,
]
