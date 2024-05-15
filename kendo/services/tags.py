from pathlib import Path
from typing import List, Optional
from rich import print, print_json
import typer
import json
from kendo.backends.crud import ISelect, IParameterizedInsert
from kendo.factory import Factory
from kendo.schemas.enums import BackendProvider
from kendo.schemas.tags import ITagAssignmentRequest
from kendo.services.common import get_kendo_config_or_raise_error


def create_tag(name: str, allowed_values: Optional[List[str]] = None):
    config_doc = get_kendo_config_or_raise_error()
    factory = Factory(config_doc)
    if config_doc["backend"]["provider"] == BackendProvider.snowflake:
        factory.backend_connection.execute("USE ROLE SYSADMIN;")

    # check for duplicate
    select_stmt_constructor: ISelect = factory.select(
        table="kendo_db.config.tags", columns=["id"], where=f"name = '{name}'"
    )
    duplicate_res = factory.backend_connection.execute(
        select_stmt_constructor.generate_statement()
    )
    if duplicate_res:
        print(f"Tag with name '{name}' already exists. Aborting...")
        raise typer.Abort()

    # INSERT into tags table
    insert_stmt_constructor: IParameterizedInsert = factory.paramized_insert(
        table="kendo_db.config.tags", columns=["name"]
    )
    factory.backend_connection.execute(
        insert_stmt_constructor.generate_statement(), (name,)
    )

    # fetch tag_id
    tag_res = factory.backend_connection.execute(
        select_stmt_constructor.generate_statement()
    )
    tag_id = None
    if tag_res and isinstance(tag_res, list):
        tag_id = tag_res[0]["ID"]
    assert tag_id is not None

    # INSERT into tags_allowed_values table
    if allowed_values:
        insert_stmt_constructor: IParameterizedInsert = factory.paramized_insert(
            table="kendo_db.config.tags_allowed_values", columns=["tag_id", "value"]
        )
        factory.backend_connection.execute_many_times(
            insert_stmt_constructor.generate_statement(),
            [(tag_id, value) for value in allowed_values],
        )

    print(f"Tag '{name}' created successfully.")

    factory.backend_connection.close_session()


def show_tags(name: Optional[str]):
    config_doc = get_kendo_config_or_raise_error()
    factory = Factory(config_doc)
    if config_doc["backend"]["provider"] == BackendProvider.snowflake:
        factory.backend_connection.execute("USE ROLE SYSADMIN;")

    select_stmt_constructor: ISelect = factory.select(table="kendo_db.config.tags")
    if name:
        select_stmt_constructor.where = f"name LIKE '%{name}%'"
    res = factory.backend_connection.execute(
        select_stmt_constructor.generate_statement()
    )

    if isinstance(res, list) and len(res) > 0:
        for i, _ in enumerate(res):
            tag_id = res[i]["ID"]
            select_stmt_constructor: ISelect = factory.select(
                table="kendo_db.config.tags_allowed_values",
                columns=["value"],
                where=f"tag_id = {tag_id}",
            )
            allowed_values = factory.backend_connection.execute(
                select_stmt_constructor.generate_statement()
            )
            if allowed_values and isinstance(allowed_values, list):
                res[i]["allowed_values"] = [
                    val["VALUE"] for val in allowed_values  # type: ignore
                ]
            del res[i]["ID"]
    print_json(data=res)
    factory.backend_connection.close_session()


def set_tag(file_path: Path):
    # TODO: handle effect on existing policies
    config_doc = get_kendo_config_or_raise_error()
    factory = Factory(config_doc)
    if config_doc["backend"]["provider"] == BackendProvider.snowflake:
        factory.backend_connection.execute("USE ROLE SYSADMIN;")

    if file_path.is_file():
        tag_assignment_data = None
        with open(file_path) as f:
            tag_assignment_data = json.load(f)

        # validate data
        tag_assignment_data = ITagAssignmentRequest.model_validate(tag_assignment_data)

        # check if tag exists
        select_stmt_constructor: ISelect = factory.select(
            table="kendo_db.config.tags",
            columns=["id"],
            where=f"name = '{tag_assignment_data.tag}'",
        )
        tag_res = factory.backend_connection.execute(
            select_stmt_constructor.generate_statement()
        )
        if not tag_res or not isinstance(tag_res, list):
            print(f"Tag with name '{tag_assignment_data.tag}' not found. Aborting...")
            raise typer.Abort()
        tag_id = tag_res[0]["ID"]

        # check if tag value is valid
        select_stmt_constructor: ISelect = factory.select(
            table="kendo_db.config.tags_allowed_values",
            columns=["value"],
            where=f"tag_id = {tag_id}",
        )
        allowed_values = factory.backend_connection.execute(
            select_stmt_constructor.generate_statement()
        )
        if (
            allowed_values
            and isinstance(allowed_values, list)
            and len(allowed_values) > 0
        ):
            allowed_values = [val["VALUE"] for val in allowed_values]  # type: ignore
            if tag_assignment_data.value not in allowed_values:
                print(
                    f"Tag value '{tag_assignment_data.value}' not allowed. Aborting..."
                )
                raise typer.Abort()

        # for each object
        for obj in tag_assignment_data.objects:
            #  TODO: check if object exists

            #  check if object has tag already
            select_stmt_constructor: ISelect = factory.select(
                table="kendo_db.config.tags_assignments",
                columns=["id"],
                where=f"tag_id = {str(tag_id)} \
                AND value = '{tag_assignment_data.value}' \
                AND obj_type = '{obj.type.value}' \
                AND obj_path = '{obj.path}'",
            )
            tag_assignment_check = factory.backend_connection.execute(
                select_stmt_constructor.generate_statement()
            )
            if tag_assignment_check:
                print(
                    f"Tag '{tag_assignment_data.tag}' with value '{tag_assignment_data.value}' already assigned to {obj.type} '{obj.path}'. Skipping..."
                )
                continue
            else:
                #  if not, insert into tag_assignments
                insert_stmt_constructor: IParameterizedInsert = (
                    factory.paramized_insert(
                        table="kendo_db.config.tags_assignments",
                        columns=["obj_type", "obj_path", "tag_id", "value"],
                    )
                )
                factory.backend_connection.execute(
                    insert_stmt_constructor.generate_statement(),
                    (obj.type.value, obj.path, tag_id, tag_assignment_data.value),
                )

        print(f"Tag '{tag_assignment_data.tag}' set successfully.")
        factory.backend_connection.close_session()
    else:
        print(f"File not found: {file_path}")
        raise typer.Abort()
