import pathlib
import pytest
from urllib.parse import quote

from geo_assistant.handlers._map_handler import MapHandler
from geo_assistant.handlers._filter import GeoFilter
from geo_assistant.handlers._exceptions import InvalidTileservTableID

FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "fixtures"


def test_map_handler_init(mock_tileserv):
    handler = MapHandler(tables=["public.parcels"])
    assert isinstance(handler, MapHandler)


def test_map_handler_raises_invalid_table(mock_tileserv):
    with pytest.raises(InvalidTileservTableID) as excinfo:
        MapHandler(tables=["not-an-id"])
    assert "not-an-id" in str(excinfo.value)


def test_tileserv_index_property(mock_tileserv, index_fixture):
    handler = MapHandler(tables=["public.parcels"])
    # Should fetch exactly our index.json
    assert handler._tileserv_index == index_fixture


def test_tileserve_table_property(mock_tileserv, table_fixture):
    handler = MapHandler(tables=["public.parcels"])
    # Should fetch exactly our public.parcels.json
    assert "public.parcels" in handler._tables_meta


def test_base_tileurl_property(mock_tileserv, table_fixture):
    handler = MapHandler(tables=["public.parcels"])
    expected = table_fixture["tileurl"] + "?columns%20%3D%20%27BBL%27"
    assert handler._get_base_tileurl("public.parcels") == expected



def test_add_and_remove_map_layer(mock_tileserv):
    handler = MapHandler(tables=["public.parcels"])
    f1 = GeoFilter(field="id", value="1", op="equal", table="public.parcels")
    handler._add_map_layer("layer1", color="red", filters=[f1], style="fill")

    # map_layers and _layer_filters should be populated
    assert "public.parcels.layer1" in handler.map_layers
    assert handler._layer_filters["layer1"] == [f1]

    layer = handler.map_layers["public.parcels.layer1"]
    # Check the minimum required keys
    assert layer["type"] == "fill"
    assert layer["color"] == "red"
    assert layer["sourcelayer"] == "public.parcels"

    cql_expr = "id = '1'"
    assert isinstance(layer["source"], list) and f"filter={quote(cql_expr)}" in layer["source"][0]

    # Now remove it
    handler._remove_map_layer("layer1")
    assert "public.parcels.layer1" not in handler.map_layers
    assert "layer1" not in handler._layer_filters


def test_reset_map(mock_tileserv):
    handler = MapHandler(tables=["public.parcels"])
    handler._add_map_layer("l1", "blue", [GeoFilter(field="a", value="1", op="equal", table="public.parcels")])
    handler._add_map_layer("l2", "green", [GeoFilter(field="b", value="2", op="equal", table="public.parcels")])
    # ensure added
    assert handler.map_layers
    handler._reset_map()
    assert handler.map_layers == {}
    assert handler._layer_filters == {}
