
import unittest
import yaml
import typer
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn


from kendo.datasource import SnowflakeDatasourceConnection
from kendo.factory import Factory
from kendo.schemas.common import ICaughtException
from kendo.schemas.enums import BackendProvider
from kendo.schemas.mapped_objs import (
    ColumnObj,
    DatabaseObj,
    PrivilegeGrantObj,
    RoleGrantObj,
    RoleObj,
    SchemaObj,
    TableObj,
    UserObj,
)
from kendo.services.common import get_kendo_config_or_raise_error
from kendo.utils.rich import colored_print


def load_yml_file(file):
    with open(file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def list_tests():
    config = load_yml_file('sample/kendo.yml')
    tests = config['tests']

    return tests


def execute_tests(datasource_connection_name: str):
    config = load_yml_file('sample/kendo.yml')
    tests = config['tests']

    snowflake_ds = SnowflakeDatasourceConnection(datasource_connection_name)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), TextColumn("([progress.percentage]{task.percentage:>3.0f}%)")) as progress:
        test_task = progress.add_task("[cyan]Executing Tests...", total=len(tests))
        for test in tests:
            progress.update(test_task, advance=1)
            progress.refresh()
            progress.update(test_task, description=f"Running test: {test['name']}")
            try:
                result = snowflake_ds.execute(test['sql'])
            except Exception as e:
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test execution error: {test['name']} ", level='error')
                colored_print("----------------------------------------------------------------")
                continue
        
            if not result:
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test failure: {test['name']} ", level='error')
                colored_print("----------------------------------------------------------------")
            else:
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test success: {test['name']} ", level='success') 
                colored_print("----------------------------------------------------------------")
        progress.update(test_task, description=f"All tests complete!")
        progress.stop()    
    snowflake_ds.close_session()
    return tests

