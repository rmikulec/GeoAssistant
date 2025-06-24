from pydantic_settings import BaseSettings
from pydantic import Field

class DefaultConfiguration(BaseSettings):
    # OpenAI Configuration
    openai_key: str = Field(default="no-key")
    parsing_model: str = Field(default="o4-mini")
    inference_model: str = Field(default="gpt-4o")
    embedding_model: str = Field(default="text-embedding-3-small")
    embedding_dims: int = Field(default=1536)

    # App Configuration
    docstore_path: str = Field(default="./docstore")
    field_def_store_version: str = Field(default="1.0.9")
    info_store_version: str = Field(default="1.0.8")
    pg_tileserv_url: str = Field(default="http://127.0.0.1:7800")
    default_table: str = Field(default="pluto")
    # Database configuration
    db_name: str = Field(default="parcelsdb")
    db_username: str = Field(default="gisuser")
    db_password: str = Field(default="pw")
    db_port: int = Field(default=5432)
    db_connection_url: str = Field(default="")
    db_base_schema: str = Field(default="base")
    db_tileserv_role: str = Field(default="pg_database_owner")
    
    #Other
    geometry_column: str = Field(default="geometry")
    srid: int = Field(default=3857)

    def __init__(self, **values):
        super().__init__(**values)
        # Set the database connection URL after the initial values have been set
        if not self.db_connection_url:
            self.db_connection_url = f"postgresql+psycopg2://{self.db_username}:{self.db_password}@localhost:{self.db_port}/{self.db_name}"


Configuration = DefaultConfiguration()