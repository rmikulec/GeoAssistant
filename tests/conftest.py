# conftest.py
import json
import pathlib

import pytest
import responses

from openai import OpenAI
from openai.types.responses import Response 

from geo_assistant.config import Configuration

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
                url=f"{Configuration.pg_tileserv_url}/{name}",
                body=path.read_bytes(),
                content_type="application/json",
                status=200,
            )
        yield rsps


@pytest.fixture
def mock_openai_client(mocker):
    """
    Fixture to mock OpenAI client for Responses API.
    - Patches OpenAI() to return a MagicMock.
    - Sets default return values for responses.create() and responses.parse().
    """
    # 1) Create a mock client matching the OpenAI spec
    mock_client = mocker.MagicMock(spec=OpenAI)

    # 2) Patch the constructor so OpenAI(api_key=…) → our mock_client
    mocker.patch("openai.OpenAI", return_value=mock_client)

    # 3) Prepare a fake Response object (use the real Pydantic model for realism)
    fake_resp = Response(
        id="resp-test",
        object="response",
        created=1234567890,
        model="test-model",
        # … fill in required fields …
    )

    # 4) Configure create() and parse() to return the fake response
    mock_client.responses.create.return_value = fake_resp
    mock_client.responses.parse.return_value = fake_resp

    return mock_client