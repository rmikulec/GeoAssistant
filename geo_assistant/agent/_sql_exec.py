import pathlib
from typing import Any, Union
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text
from jinja2 import Template


from geo_assistant.logging import get_logger
logger = get_logger(__name__)

TEMPATE_PATH = pathlib.Path(__file__).resolve().parent / "templates"

def execute_template_sql(
    template_name: str,
    engine: Union[Engine, Connection],
    *args: Any,
    **kwargs: Any
) -> None:
    """
    1. Load a Jinja2 template by name
    2. Render it with positional args + named kwargs
    3. Execute the SQL against the given SQLAlchemy engine/connection

    Args:
        template_name (str): name of the template found in the `./templates` directory
        engine (Union[Engine, Connection]): SQLAlchemy Engine or Connection
        *args, **kwargs: Any additional arguements to be injected into the template

    """
    # 1) Load template
    template: Template = Template(
        source=(TEMPATE_PATH / (template_name + ".sql")).read_text()
    )
    # 2) Render SQL
    sql: str = template.render(*args, **kwargs).strip()
    logger.debug(sql)
    # 3) Execute
    # If it's an Engine, open a connection + transaction
    if isinstance(engine, Engine):
        with engine.begin() as conn:       # begin() will commit on success
            conn.execute(text(sql))
    else:
        # It's already a Connection
        engine.execute(text(sql))
