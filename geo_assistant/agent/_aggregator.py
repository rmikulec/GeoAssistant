from typing import Literal
from pydantic import BaseModel, Field, ConfigDict, create_model

# all supported aggregation functions
AggregatorOperator = Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']

class Aggregator(BaseModel):
    operator: AggregatorOperator = Field(..., description="Aggregation function")
    alias:   str | None            = Field(
        None,
        description="Optional alias for the aggregated output"
    )

    # use operator as discriminator
    model_config = ConfigDict(discriminator='operator')

    @classmethod
    def _build_aggregator(cls, fields_enum):
        return create_model(
            f"{cls.__name__}Dynamic",
            __base__=cls,
            column=(fields_enum, ...)
        )


class CountAggregator(Aggregator):
    operator: Literal['COUNT']
    column:   str | Literal['*']    = Field(
        '*',
        description="Column to count (or '*' for all rows)"
    )
    distinct: bool                  = Field(
        False,
        description="Whether to count only distinct values"
    )


class SumAggregator(Aggregator):
    operator: Literal['SUM']
    column:   str                   = Field(..., description="Column to sum")


class AvgAggregator(Aggregator):
    operator: Literal['AVG']
    column:   str                   = Field(..., description="Column to average")


class MinAggregator(Aggregator):
    operator: Literal['MIN']
    column:   str                   = Field(..., description="Column to take minimum of")


class MaxAggregator(Aggregator):
    operator: Literal['MAX']
    column:   str                   = Field(..., description="Column to take maximum of")


# The union type you’ll actually use:
SQLAggregators: list[Aggregator] = [
    CountAggregator,
    SumAggregator,
    AvgAggregator,
    MinAggregator,
    MaxAggregator,
]
