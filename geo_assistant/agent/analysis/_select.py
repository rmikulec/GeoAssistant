from pydantic import BaseModel, Field, create_model
from enum import Enum
from typing import Optional

class _Column(BaseModel):
    """
    Base Aggregator class, requiring an `operator` for each subclass, and providing an `alias`
        field for each subclass

    Base class also has a private build method to inject fields into a `column` model field
    """
    column: Enum
    alias: Optional[str] = Field(
        None,
        description="Optional alias for the aggregated output"
    )

    @classmethod
    def _build(cls, fields_enum):
        """
        Private method to inject Fields Enum into a `column` model field
        """
        return create_model(
            cls.__name__.removeprefix('_'),
            __base__=cls,
            column=(fields_enum, ...)
        )