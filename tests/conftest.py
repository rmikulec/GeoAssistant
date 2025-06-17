# conftest.py
import json
import pathlib

import pytest
import responses

FIXTURE_DIR = pathlib.Path(__file__).parent / "data"


def load_json(name: str):
    return json.loads((FIXTURE_DIR / name).read_text())


@pytest.fixture
def index_fixture():
    return load_json("index.json")


@pytest.fixture
def table_fixture():
    return load_json("public.parcels.json")


@pytest.fixture
def mock_tileserv():
    """
    Intercepts HTTP GETs to localhost:7800/public.parcels.json
    and /index.json, returning the contents of tests/fixtures/.
    """
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        for name in ("public.parcels.json", "index.json"):
            path = FIXTURE_DIR / name
            rsps.add(
                method=responses.GET,
                url=f"http://localhost:7800/{name}",
                body=path.read_bytes(),
                content_type="application/json",
                status=200,
            )
        yield rsps
