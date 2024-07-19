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
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def list_tests():
    config = load_yml_file("sample/kendo.yml")
    tests = config["tests"]

    return tests


def execute_tests(datasource_connection_name: str):
    config = load_yml_file("sample/kendo.yml")
    tests = config["tests"]

    snowflake_ds = SnowflakeDatasourceConnection(datasource_connection_name)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("([progress.percentage]{task.percentage:>3.0f}%)"),
    ) as progress:
        test_task = progress.add_task("[cyan]Executing Tests...", total=len(tests))
        for test in tests:
            progress.update(test_task, advance=1)
            progress.refresh()
            progress.update(test_task, description=f"Running test: {test['name']}")
            try:
                result = snowflake_ds.execute(test["sql"])
                # Ensure result is in the expected format
                if not isinstance(result, list):
                    result = []  # or handle accordingly if result is not as expected
            except Exception as e:
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test execution error: {test['name']} ", level="error")
                colored_print(str(e), level="error")
                colored_print("----------------------------------------------------------------")
                continue
            
            # Check if result matches expected output
            expected_result = test.get("expected", [])
            if compare_results(result, expected_result):
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test success: {test['name']} ", level="success")
                colored_print("----------------------------------------------------------------")
            else:
                colored_print("----------------------------------------------------------------")
                colored_print(f"Test failed: {test['name']} ", level="error")
                colored_print("----------------------------------------------------------------")

            print("SQL: ", test["sql"])
            if "expected" in test:
                print("EXPECTED: ", test["expected"])
            print("RESULT: ", result)
        progress.update(test_task, description=f"All tests complete!")
        progress.stop()
    snowflake_ds.close_session()
    return tests

def normalize_row(row):
    return {k.lower(): v for k, v in row.items()}

def compare_results(result, expected):
    # Treat None and [] as equivalent
    if (result is None and expected == []) or (expected is None and result == []):
        return True
    
    if not isinstance(result, list) or not isinstance(expected, list) or len(result) != len(expected):
        return False
    
    # Normalize row keys to lowercase for both actual and expected results
    normalized_result = [normalize_row(row) for row in result]
    normalized_expected = [normalize_row(row) for row in expected]

    # Sort both lists of dictionaries for comparison
    normalized_result.sort(key=lambda x: sorted(x.items()))
    normalized_expected.sort(key=lambda x: sorted(x.items()))

    return normalized_result == normalized_expected
