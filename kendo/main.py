import typer
from kendo.snowflake.app import app as snowflake_app

app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def callback():
    """
    Manage ABAC.
    """

app.add_typer(snowflake_app, name="snowflake")
