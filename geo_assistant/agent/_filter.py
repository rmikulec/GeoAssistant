from typing import Union, List
from typing_extensions import Literal
from pydantic import BaseModel, ConfigDict, Field, create_model

# our shared literal of all operators
Operator = Literal[
    '=', '!=', '>', '<', '>=', '<=', 
    'IN', 'NOT IN', 
    'BETWEEN', 
    'LIKE', 'ILIKE', 
    'IS NULL', 'IS NOT NULL'
]

def _quote(val: Union[str, int, float]) -> str:
    if isinstance(val, str):
        # escape single quotes for CQL
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    return str(val)

class _FilterItem(BaseModel):
    column: str = Field(..., description="Column name to filter")
    operator: Operator = Field(..., description="Comparison operator")

    # enable discriminated union on `operator`
    model_config = ConfigDict(discriminator='operator')

    def to_cql(self) -> str:
        """Fallback for filters without extra data"""
        return f"{self.column} {self.operator}"

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

    def to_cql(self) -> str:
        return f"{self.column} {self.operator} {_quote(self.value)}"

class _ListFilter(_FilterItem):
    operator: Literal['IN', 'NOT IN']
    value_list: List[Union[str, int, float]] = Field(
        ..., description="List of values for IN / NOT IN"
    )

    def to_cql(self) -> str:
        items = ", ".join(_quote(v) for v in self.value_list)
        return f"{self.column} {self.operator} ({items})"

class _BetweenFilter(_FilterItem):
    operator: Literal['BETWEEN']
    lower: Union[str, int, float]
    upper: Union[str, int, float]

    def to_cql(self) -> str:
        return (
            f"{self.column} BETWEEN "
            f"{_quote(self.lower)} AND {_quote(self.upper)}"
        )

class _NullFilter(_FilterItem):
    operator: Literal['IS NULL', 'IS NOT NULL']

    def to_cql(self) -> str:
        return f"{self.column} {self.operator}"

# List of allowed filter types
SQLFilters: list[type[_FilterItem]] = [
    _ValueFilter,
    _ListFilter,
    _BetweenFilter,
    _NullFilter,
]
