from pathlib import Path
import typer
from typing_extensions import Annotated
from typing import List, Optional

from kendo.schemas.enums import BackendProvider
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

app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def callback():
    pass


# @app.command()
# def develop():
#     """
#     Developer command.
#     """
#     print("Hello World!")


@app.command()
def configure(
    backend_provider: Annotated[
        Optional[BackendProvider], typer.Option()
    ] = BackendProvider.snowflake,
    datasource_connection_name: Annotated[Optional[str], typer.Option()] = "default",
):
    """
    Setup local config and backend database required for managing Access Control.
    """
    assert backend_provider is not None
    assert datasource_connection_name is not None
    setup_config_database(backend_provider, datasource_connection_name)


@app.command()
def session_details(
    datasource_connection_name: Annotated[Optional[str], typer.Option()] = "default",
):
    """
    Show Warehouse and Roles of current session.
    """
    assert datasource_connection_name is not None
    show_session_details(datasource_connection_name)


@app.command()
def show_missing_grants(
    datasource_connection_name: Annotated[Optional[str], typer.Option()] = "default",
):
    """
    Show missing Grants of Role in current session.
    """
    assert datasource_connection_name is not None
    show_missing_grants_service(datasource_connection_name)


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
    scan_infra_service()


@app.command()
def create_tag(
    name: Annotated[str, typer.Argument()],
    allowed_values: Annotated[Optional[List[str]], typer.Argument()] = None,
):
    """
    Create a Tag, optionally with allowed values.
    """
    create_tag_service(name, allowed_values)


@app.command()
def show_tags(
    name_like: Annotated[Optional[str], typer.Option()] = None,
):
    """
    Show Tags.
    """
    show_tags_service(name_like)


@app.command()
def set_tag(
    file_path: Annotated[Path, typer.Argument()],
):
    """
    Set Tag to objects.
    """
    set_tag_service(file_path)
