"""
File contains logic / classes to build out Filter classes for OpenAI structured outputs

This allows to creation of a json schema, complete with enums, for different types of WHERE
    operations
"""

from abc import ABC
from typing import Union, List
from typing_extensions import Literal
from pydantic import BaseModel, ConfigDict, Field, create_model

# literal of all operators
Operator = Literal[
    '=', '!=', '>', '<', '>=', '<=', 
    'IN', 'NOT IN', 
    'BETWEEN', 
    'LIKE', 'ILIKE', 
    'IS NULL', 'IS NOT NULL'
]


class _FilterItem(BaseModel, ABC):
    """
    Base Filter class, requiring an `operator` for each subclass, and providing an `alias`
        field for each subclass

    Base class also has a private build method to inject fields into a `column` model field
    """
    operator: Operator = Field(..., description="Comparison operator")
    column: str = Field(..., description="Column name to filter")

    # enable discriminated union on `operator`
    model_config = ConfigDict(discriminator='operator')

    @classmethod
    def _build_filter(cls, fields_enum):
        """
        Private method to inject Fields Enum into a `column` model field
        """
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            column=(fields_enum, ...)
        )

"""
Filter classes each for a different type of WHERE operation. To add more, extend the base
    class and add to the list at the bottom
"""


class _ValueFilter(_FilterItem):
    operator: Literal['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE']
    value: Union[str, int, float] = Field(
        ..., description="Single value for comparisons"
    )


class _ListFilter(_FilterItem):
    operator: Literal['IN', 'NOT IN']
    value_list: List[Union[str, int, float]] = Field(
        ..., description="List of values for IN / NOT IN"
    )


class _BetweenFilter(_FilterItem):
    operator: Literal['BETWEEN']
    lower: Union[str, int, float]
    upper: Union[str, int, float]


class _NullFilter(_FilterItem):
    operator: Literal['IS NULL', 'IS NOT NULL']


# List of allowed filter types
SQLFilters: list[type[_FilterItem]] = [
    _ValueFilter,
    _ListFilter,
    _BetweenFilter,
    _NullFilter,
]
