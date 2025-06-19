from typing import Union, List
from typing_extensions import Literal
from pydantic import BaseModel, ConfigDict, Field,  create_model

# our shared literal of all operators
Operator = Literal[
    '=', '!=', '>', '<', '>=', '<=', 
    'IN', 'NOT IN', 
    'BETWEEN', 
    'LIKE', 'ILIKE', 
    'IS NULL', 'IS NOT NULL'
]

class _FilterItem(BaseModel):
    column: str = Field(..., description="Column name to filter")
    operator: Operator = Field(..., description="Comparison operator")

    # enable discriminated union on `operator`
    model_config = ConfigDict(discriminator='operator')


    @classmethod
    def _build_filter(cls, fields_enum):
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            column=(fields_enum, ...)
        )


class _ValueFilter(_FilterItem):
    operator: Literal['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE']
    value: Union[str, int, float] = Field(
        ..., description="Single value for comparisons"
    )


class _ListFilter(_FilterItem):
    operator: Literal['IN', 'NOT IN']
    values: List[Union[str, int, float]] = Field(
        ..., description="List of values for IN / NOT IN"
    )


class _BetweenFilter(_FilterItem):
    operator: Literal['BETWEEN']
    lower: Union[str, int, float]
    upper: Union[str, int, float]


class _NullFilter(_FilterItem):
    operator: Literal['IS NULL', 'IS NOT NULL']
    # no extra fields allowed


SQLFilters: list[_FilterItem] = [
    _ValueFilter,
    _ListFilter,
    _BetweenFilter,
    _NullFilter,
]
