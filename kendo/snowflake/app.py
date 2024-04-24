from pathlib import Path
import typer
from typing_extensions import Annotated
from typing import List, Optional
from rich import print

from .services.security_clearance import (
    show_session_details,
    show_missing_grants as show_missing_grants_service,
    show_required_grants as show_required_grants_service,
)
from .services.configuration import (
    setup_config_database,
    scan_infra as scan_infra_service,
)
from .services.tags import (
    create_tag as create_tag_service,
    show_tags as show_tags_service,
    set_tag as set_tag_service,
)

app = typer.Typer()
snowflake_connection_name = "default"


@app.callback()
def callback(connection_name: str = "default"):
    global snowflake_connection_name
    snowflake_connection_name = connection_name


# @app.command()
# def develop():
#     """
#     Developer command.
#     """
#     print("Hello World!")


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
def scan_infra():
    """
    Scan Snowflake infrastructure.
    """
    scan_infra_service(snowflake_connection_name)


@app.command()
def create_policy(
    policy_name: Annotated[str, typer.Argument()]
):
    """
    Check Policy.
    """
    create_policy(snowflake_connection_name, policy_name)


@app.command()
def configure():
    """
    Setup database and schema in Snowflake required for managing Access Control.
    """
    setup_config_database(snowflake_connection_name)


@app.command()
def create_tag(
    name: Annotated[str, typer.Argument()],
    allowed_values: Annotated[Optional[List[str]], typer.Argument()] = None,
):
    """
    Create a Tag, optionally with allowed values.
    """
    create_tag_service(snowflake_connection_name, name, allowed_values)


@app.command()
def show_tags(
    name_like: Annotated[Optional[str], typer.Option()] = None,
):
    """
    Show Tags.
    """
    show_tags_service(snowflake_connection_name, name_like)


@app.command()
def set_tag(
    file_path: Annotated[Path, typer.Argument()],
):
    """
    Set Tag to objects.
    """
    set_tag_service(snowflake_connection_name, file_path)
