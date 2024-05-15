import os

import tomli
import typer

from kendo.utils.rich import colored_print


def get_kendo_config_or_raise_error():
    # check if kendo local config file exists
    kendo_config_dir = os.path.join(os.path.expanduser("~"), ".kendo")
    kendo_config_path = os.path.join(kendo_config_dir, "config.toml")
    config_doc = None
    if not os.path.exists(kendo_config_path):
        colored_print(
            "Kendo local config file not found. Please run `kendo configure` first to setup local config.",
            level="error",
        )
        raise typer.Abort()
    else:
        with open(kendo_config_path, "rb") as f:
            config_doc = tomli.load(f)
    return config_doc
