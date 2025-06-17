from pydantic_settings import BaseSettings
from pydantic import Field

class DefaultConfiguration(BaseSettings):
    # OpenAI Configuration
    openai_key = Field(default="no-key")
    parsing_model = Field(default="o4-mini")
    inference_model = Field(default="gpt-4o")

    # App Configuration
    data_store_path = Field(default="./pluto/field_def_index-test2")
    pg_tileserv_url = Field(default="http://127.0.0.1:7800")
    # Database configuration
    db_name: str = Field(default="parcelsdb")
    db_username: str = Field(default="gisuser")
    db_password: str = Field(default="pw")
    db_port: str = Field(default=5432)
    db_connection_url: str = Field(default="")

    def __init__(self, **values):
        super().__init__(**values)
        # Set the database connection URL after the initial values have been set
        if not self.db_connection_url:
            self.db_connection_url = f"postgresql+psycopg2://{self.db_username}:{self.db_password}@localhost:{self.db_port}/{self.db_name}"


Configuration = DefaultConfiguration()