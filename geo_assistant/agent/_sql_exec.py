import pathlib
from typing import Any
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text
from jinja2 import Template


TEMPATE_PATH = pathlib.Path(__file__).resolve().parent / "templates"

def execute_template_sql(
    template_name: str,
    engine: Engine | Connection = None,
    *args: Any,
    **kwargs: Any
) -> None:
    """
    1. Load a Jinja2 template by name
    2. Render it with positional args + named kwargs
    3. Execute the SQL against the given SQLAlchemy engine/connection

    :param engine: SQLAlchemy Engine or Connection
    :param jinja_env: Jinja2 Environment pointed at your templates folder
    :param template_name: filename, e.g. 'buffer_step.sql.j2'
    :param args: positional args passed into template.render()
    :param kwargs: keyword args passed into template.render()
    """
    # 1) Load template
    template: Template = Template(
        source=(TEMPATE_PATH / (template_name + ".sql")).read_text()
    )
    print(kwargs)
    # 2) Render SQL
    sql: str = template.render(*args, **kwargs).strip()
    if engine is None:
        print(sql)
        return sql
    # 3) Execute
    # If it's an Engine, open a connection + transaction
    if isinstance(engine, Engine):
        with engine.begin() as conn:       # begin() will commit on success
            conn.execute(text(sql))
    else:
        # It's already a Connection
        engine.execute(text(sql))
