import typer

from .services.security_clearance import (
    show_session_details,
    show_current_role_grants,
    show_required_grants as show_required_grants_service,
)

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
def session_role_grants():
    """
    Show Grants of Role in current session.
    """
    show_current_role_grants(snowflake_connection_name)


@app.command()
def show_required_grants():
    """
    Show Grants required to operate CLI.
    """
    show_required_grants_service()
