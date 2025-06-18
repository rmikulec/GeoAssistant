import pytest
from pydantic import ValidationError
from urllib.parse import quote

from geo_assistant.handlers._filter import GeoFilter


@pytest.mark.parametrize(
    "field,value,op,expected",
    [
        ("age", 30, "greaterThan", "age > 30"),
        ("score", 0, "lessThanOrEqual", "score <= 0"),
        ("flag", True, "notEqual", "flag <> true"),
    ],
)
def test_to_cql_numeric_and_bool(field, value, op, expected):
    gf = GeoFilter(field=field, value=value, op=op, table="public.parcels")
    assert gf._to_cql() == quote(expected, safe="")


def test_to_cql_string_escaping():
    # Ensure single quotes are doubled and URL-encoded
    gf = GeoFilter(field="name", value="O'Reilly", op="equal", table="public.parcels")
    raw = "name = 'O''Reilly'"
    assert gf._to_cql() == quote(raw, safe="")


def test_to_cql_mixed_ops():
    # Test other operators
    gf1 = GeoFilter(field="x", value=5, op="lessThan", table="public.parcels")
    assert gf1._to_cql() == quote("x < 5", safe="")

    gf2 = GeoFilter(field="y", value=10, op="greaterThanOrEqual", table="public.parcels")
    assert gf2._to_cql() == quote("y >= 10", safe="")


@pytest.mark.parametrize(
    "field,value,op,expected_sql",
    [
        ("age", 30, "greaterThan", '"age" > 30'),
        ("score", 0, "lessThanOrEqual", '"score" <= 0'),
        ("flag", False, "equal", '"flag" = FALSE'),
        ("x", 5, "notEqual", '"x" != 5'),
    ],
)
def test_to_sql(field, value, op, expected_sql):
    gf = GeoFilter(field=field, value=value, op=op, table="public.parcels")
    assert gf._to_sql() == expected_sql


def test_to_sql_string_escaping():
    # Ensure single quotes are doubled in SQL literal
    gf = GeoFilter(field="col", value="a'b", op="equal", table="public.parcels")
    assert gf._to_sql() == "\"col\" = 'a''b'"


def test_invalid_op_raises():
    with pytest.raises(ValidationError):
        GeoFilter(field="f", value=1, op="invalidOp", table="public.parcels")
