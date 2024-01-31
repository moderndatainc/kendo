import typer

from .services.security_clearance import (
    show_session_details,
    show_missing_grants as show_missing_grants_service,
    show_required_grants as show_required_grants_service,
)
from .services.configuration import setup_config_database

app = typer.Typer()
snowflake_connection_name = "default"


@app.callback()
def callback(connection_name: str = "default"):
    global snowflake_connection_name
    snowflake_connection_name = connection_name


@app.command()
def session_details():
    """
    Show Warehouse and Role of current session.
    """
    show_session_details(snowflake_connection_name)


@app.command()
def show_missing_grants():
    """
    Show missing Grants of Role in current session.
    """
    show_missing_grants_service(snowflake_connection_name)


@app.command()
def show_required_grants():
    """
    Show Grants required to operate CLI.
    """
    show_required_grants_service()


@app.command()
def configure():
    """
    Setup database and schema in Snowflake required for managing Access Control.
    """
    setup_config_database(snowflake_connection_name)
