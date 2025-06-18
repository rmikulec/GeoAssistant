from typing import List, Union
from typing_extensions import Literal
from pydantic import BaseModel, Field, model_validator

Operator = Literal[
    '=', '!=', '>', '<', '>=', '<=', 
    'IN', 'NOT IN', 
    'BETWEEN', 
    'LIKE', 'ILIKE', 
    'IS NULL', 'IS NOT NULL'
]

class FilterItem(BaseModel):
    column: str = Field(..., description="Column name to filter")
    operator: Operator = Field(..., description="Comparison operator")
    
    # used for single‐value operators (=, !=, >, <, >=, <=, LIKE, ILIKE)
    value: Union[str, int, float, None] = Field(
        None,
        description="Single value for comparisons"
    )
    # used for IN / NOT IN
    values: List[Union[str, int, float]] = Field(
        None,
        description="List of values for IN / NOT IN"
    )
    # used for BETWEEN
    range: List[Union[str, int, float]] = Field(
        None,
        min_items=2, max_items=2,
        description="[low, high] for BETWEEN"
    )

"""
    @model_validator(mode="after")
    def check_fields_match_operator(cls, values):
        op = values.get('operator')
        val = values.get('value')
        vals = values.get('values')
        rng = values.get('range')

        if op in ('IN', 'NOT IN') and not vals:
            raise ValueError("`values` must be set for IN/NOT IN")
        if op == 'BETWEEN' and not rng:
            raise ValueError("`range` must be set for BETWEEN")
        if op in ('=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE') and val is None:
            raise ValueError(f"`value` must be set for operator {op}")
        if op in ('IS NULL', 'IS NOT NULL') and any([val, vals, rng]):
            raise ValueError(f"No extra fields allowed for operator {op}")
        return values
"""