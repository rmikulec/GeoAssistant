import pathlib
from typing import Any, Union, List, Dict, Optional
from sqlalchemy.engine import Engine, Connection, Result
from sqlalchemy import text
from jinja2 import Template

from geo_assistant.logging import get_logger
logger = get_logger(__name__)

# NOTE: The constant name preserves the original typo to avoid breaking references.
TEMPATE_PATH = pathlib.Path(__file__).resolve().parent / "templates"

def execute_template_sql(
    template_name: str,
    engine: Union[Engine, Connection],
    *args: Any,
    **kwargs: Any
) -> Optional[List[Dict[str, Any]]]:
    """
    1. Load a Jinja2 template by name
    2. Render it with positional args + named kwargs
    3. Execute the SQL against the given SQLAlchemy engine/connection
    4. If the statement returns rows, capture and return them as a list of dicts

    Args:
        template_name (str): name of the template found in the `./templates` directory
        engine (Union[Engine, Connection]): SQLAlchemy Engine or Connection
        *args, **kwargs: Any additional arguments to be injected into the template

    Returns:
        Optional[List[Dict[str, Any]]]: Rows returned by the query as a list of dicts,
        or None if no rows were returned (e.g., DDL statements)
    """
    # 1) Load template
    template: Template = Template(
        source=(TEMPATE_PATH / (template_name + ".sql")).read_text(), trim_blocks=True, lstrip_blocks=True
    )
    # 2) Render SQL
    sql: str = template.render(*args, **kwargs).strip()
    logger.debug("Executing SQL:\n%s", sql)

    def _process_result(result: Result) -> Optional[List[Dict[str, Any]]]:
        # Only return rows if the SQL returned any
        if not result.returns_rows:
            return None
        # Use mappings() to get rows as dict-like objects
        mappings = result.mappings().all()
        return [dict(row) for row in mappings]

    # 3) Execute and capture results
    if isinstance(engine, Engine):
        with engine.begin() as conn:  # begin() will commit on success
            result = conn.execute(text(sql))
            return _process_result(result)
    else:
        # It's already a Connection
        result = engine.execute(text(sql))  # noqa: DBAPI
        return _process_result(result)
