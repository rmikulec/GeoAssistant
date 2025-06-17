from pydantic import BaseModel
from typing import Any, Literal
from urllib.parse import quote

class GeoFilter(BaseModel):
    field: str
    value: Any
    op: Literal["equal", "greaterThan", "lessThan", "greaterThanOrEqual", "lessThanOrEqual", "notEqual", "contains"]


    def _to_cql(self):
        op_mapping = {
            "equal": "=",
            "greaterThan": ">",
            "lessThan": "<",
            "greaterThanOrEqual": ">=",
            "lessThanOrEqual": "<=",
            "notEqual": "<>",
            "contains": "LIKE"
        }

        cql_op = op_mapping[self.op]
        if self.op=='contains':
            value = f"%{self.value}%"
        else:
            value = self.value

        # format the value as a CQL literal
        if isinstance(value, str):
            # escape single quotes by doubling them
            esc = value.replace("'", "''")
            literal = f"'{esc}'"
        elif isinstance(value, bool):
            literal = "true" if value else "false"
        else:
            literal = str(value)

        # build the raw CQL expression

        expr = f"{self.field} {cql_op} {literal}"

        # URL-encode it (spaces → %20, quotes → %27, etc.)
        return quote(expr, safe="")
    
    def _to_sql(self):
        op_mapping = {
            "equal": "=",
            "greaterThan": ">",
            "lessThan": "<",
            "greaterThanOrEqual": ">=",
            "lessThanOrEqual": "<=",
            "notEqual": "!=",
            "contains": "~"
        }

        sql_op = op_mapping[self.op]

        # format the value as a CQL literal
        if isinstance(self.value, str):
            # escape single quotes by doubling them
            esc = self.value.replace("'", "''")
            literal = f"'{esc}'"
        elif isinstance(self.value, bool):
            literal = "TRUE" if self.value else "FALSE"
        else:
            literal = str(self.value)

        return f'"{self.field}" {sql_op} {literal}'