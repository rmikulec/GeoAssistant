import requests
from typing import Self, Optional, Any, Sequence
from pydantic import BaseModel
from sqlalchemy import Engine, text
from sqlalchemy.exc import ProgrammingError

from geo_assistant.agent._sql_exec import execute_template_sql
from geo_assistant.config import Configuration


class Table(BaseModel):
    name: str
    schema: str
    columns: list[str]
    index_url: str
    tile_url: str
    bounds: dict[str, float]
    
    #Set later after creation
    geometry_type: str = None

    def filter(self, fields: list[str]) -> Self:
        new_table = self.model_copy()
        new_table.columns = [col for col in self.columns if col in fields]
        return new_table

    def _drop(self, engine: Engine) -> None:
        """
        Drops the table if it exists.

        Args:
            - engine: SQLAlchemy Engine connected to your PostGIS database.
            - table_name: Name of the table to drop (no schema).
            - schema: Schema where the table lives (defaults to 'public').
        """
        execute_template_sql(
            engine=engine,
            template_name="drop",
            output_tables=[self.name]
        )
    
    def _create_spatial_index(self, engine: Engine):
        with engine.begin() as conn:
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS "{self.schema}".{self.name}_geometry_gist_idx ON "public"."{self.name}" USING GIST (geometry);'

                )
            )
            conn.execute(
                text(f'ANALYZE "{self.schema}"."{self.name}"')
            )
    
    def _postprocess(self, engine: Engine):
        """
        After CTAS, normalize the geom column, register it,
        then grant/select, add GIST index, and analyze.
        """

        # 2) grant, index, analyze
        with engine.begin() as conn:
            conn.execute(
                text(
                    (
                        "SELECT Populate_Geometry_Columns("
                        f"'{self.schema}.{self.name}'::regclass"
                        ");"
                    )
                )
            )
            conn.execute(
                text(f"GRANT SELECT ON {self.schema}.{self.name} TO public")
            )


class TableRegistry:

    def __init__(self):
        self.tables: dict[Table] = {}

    
    @property
    def schemas(self):
        return {
            table.schema
            for table in self.tables.values()
        }

    @staticmethod
    def _get_geometry_type(
        engine: Engine, 
        schema: str,
        table: str, 
        geom_col: str = Configuration.geometry_column
    ) -> str:
        """
        Return the PostGIS geometry type for the specified table.

        Args:
            conn_params: dict of connection params for psycopg2.connect, e.g.
                        {"host":"localhost","port":5432,"dbname":"yourdb",
                        "user":"you","password":"secret"}
            table:       name of the table to inspect
            schema:      schema where the table lives (default "public")
            geom_col:    name of the geometry column (default "geom")

        Returns:
            A string like "ST_Point", "ST_Polygon", etc., or None if not found.
        """
        stmt = text(f"""
            SELECT DISTINCT ST_GeometryType("{geom_col}") AS geom_type
            FROM "{schema}"."{table}"
            WHERE "{geom_col}" IS NOT NULL
            LIMIT 1
        """)

        with engine.connect() as conn:
            try:
                result = conn.execute(stmt)
                row = result.fetchone()
                return row[0].removeprefix('ST_') if row else "NotFound"
            except ProgrammingError:
                return "NotFound"


    @staticmethod
    def _extract_table_from_tileserv(
        index_info: dict,
        metadata: dict,
    ) -> Table:
        if "properties" in metadata:
            columns = [prop['name'] for prop in metadata['properties']]
        else:
            columns = []

        if "bounds" in metadata:
            bounds_data = metadata['bounds']
            bounds = {
                "west": bounds_data[0],
                "south": bounds_data[1],
                "east":  bounds_data[2],
                "north": bounds_data[3],
            }
        else:
            bounds = {
                "west": -90,
                "south": -180,
                "east":  90,
                "north": 180,
            }

        return Table(
            name=index_info['name'],
            schema=index_info['schema'],
            index_url=index_info['detailurl'],
            tile_url=metadata['tileurl'],
            columns=columns,
            bounds=bounds
        )

    @classmethod
    def load_from_tileserv(cls, engine: Engine) -> Self:
        index = requests.get(
            f"{Configuration.pg_tileserv_url}/index.json"
        ).json()

        instance = cls()
        for id_, info in index.items():
            metadata = requests.get(
                info['detailurl']
            ).json()
            table = cls._extract_table_from_tileserv(
                info, metadata
            )
            table.geometry_type = cls._get_geometry_type(
                engine=engine,
                schema=info['schema'],
                table=info['name'],
            )
            instance.tables[id_] = table
        return instance


    def register(self, name: str, engine: Engine) -> Table:
        # Search index:
        index = requests.get(
            f"{Configuration.pg_tileserv_url}/index.json"
        ).json()
        

        for id_, info in index.items():
            if info['name'] == name:
                metadata = requests.get(
                    info['detailurl']
                ).json()
                table = self._extract_table_from_tileserv(
                    info, metadata
                )
                table.geometry_type = self._get_geometry_type(
                    engine=engine,
                    schema=info['schema'],
                    table=info['name'],
                )
                self.tables[id_] = table
    
        return self.tables[id_]

    def unregister(self, name: str, engine: Engine):
        for id_, table in self.tables.items():
            if table.name == name:
                table._drop(engine)
                del self.tables[id_]
                return
    
    def cleanup(self, engine: Engine):
        for temp_id in self.temp_tables:
            self.tables[temp_id]._drop(engine)
            del self.tables[temp_id]


    def __getitem__(self, key) -> list[Table]:
        """
        Example usage

        # raw lookup by table name
        users_table = coll[('table', 'users')]

        # raw lookup by analysis name
        sales_analysis = coll[('analysis', 'monthly_sales')]

        # filter *all* tables for a given set of fields
        filtered = coll[('fields', ['id', 'amount'])]  

        # combine table name + field filter
        user_fields = coll[
            ('table', 'users'),
            ('fields', ['id', 'last_login'])
        ]  # returns a list of filtered Table objects

        # combine analysis name + field filter
        sales_fields = coll[
            ('analysis', 'monthly_sales'),
            ('fields', ['region', 'total'])
        ]
        """
        if not isinstance(key, tuple):
            raise TypeError(
                "Indexing must be a tuple of (kind, value) pairs, "
                "with kind in {'table','analysis','fields'}"
            )

        # normalize to a sequence of 2-tuples
        if len(key) == 2 and isinstance(key[0], str):
            conds = (key,)
        else:
            if all(isinstance(item, tuple) and len(item) == 2 for item in key):
                conds = key
            else:
                raise TypeError(
                    "Indexing tuple must be either "
                    "(kind, value) or a tuple of such pairs"
                )

        allowed = {'table', 'analysis', 'fields', 'schema'}
        # start with all table objects
        candidates = list(self.tables.values())

        for kind, val in conds:
            if kind not in allowed:
                raise KeyError(f"Unknown index kind {kind!r}; must be one of {allowed}")

            if kind == 'schema':
                candidates = [t for t in candidates if t.schema == val]

            elif kind == 'table':
                candidates = [t for t in candidates if t.name == val]

            elif kind == 'analysis':
                candidates = [t for t in candidates if t.analysis == val]

            elif kind == 'fields':
                if not isinstance(val, Sequence) or isinstance(val, str):
                    raise TypeError("For 'fields', provide a list/tuple of field names")
                field_list = list(val)

                # for fields, replace each table with its filtered version (if it yields any columns)
                new_candidates = []
                for table in candidates:
                    filtered = table.filter(field_list)
                    if filtered.columns:
                        new_candidates.append(filtered)
                candidates = new_candidates

        return candidates

    def drop_schema(self, engine: Engine, schema_name: str) -> None:
        """
        Drops a PostgreSQL/PostGIS schema and all contained objects.

        Args:
            - engine: an initialized SQLAlchemy Engine
            - schema_name: name of the schema to drop
        """
        sql = text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;')
        with engine.begin() as conn:
            conn.execute(sql)

    def verify_fields(self, field_results: list[dict]):
        updated_results = []

        for table in self.tables.values():
            for column in table.columns:
                for field_result in field_results:
                    if column.lower() == field_result['name'].lower():
                        temp = field_result.copy()
                        temp.pop('name')
                        updated_results.append(
                            {
                                "name": column,
                                **temp
                            }
                        )

        return updated_results
