import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm.session import Session
import typer

from kendo.snowflake.connection import get_credentials_from_toml
from kendo.snowflake.models.base import Base
from kendo.snowflake.schemas.enums import BackendDB
from kendo.snowflake.utils.rich import colored_print


# get sqlalchemy session
def get_config_db_session(config_db: BackendDB, **kwargs):
    engine = None
    if config_db == "snowflake":
        connection_name = kwargs.get("connection_name", "default")
        creds = get_credentials_from_toml(connection_name)
        URL = "snowflake://{user}:{password}@{account}/{database}/".format(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            database="kendo_db",
        )
        engine = create_engine(URL)
    elif config_db == "duckdb":
        kendo_local_dir = os.path.join(os.path.expanduser("~"), ".kendo")
        db_path = os.path.join(kendo_local_dir, "kendo_db.duckdb")
        URL = f"duckdb:///{db_path}"
        engine = create_engine(URL)
        Base.metadata.create_all(engine)
    else:
        colored_print("Invalid backend.", level="error")
        typer.Abort()

    return Session(bind=engine), engine


def close_config_db_session(session, engine):
    session.close()
    engine.dispose()
