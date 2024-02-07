from pathlib import Path
from typing import List, Optional
from rich import print, print_json
import typer
import json
from kendo.snowflake.connection import (
    get_session,
    close_session,
    execute_anonymous_block,
    execute,
)
from kendo.snowflake.schemas.tags import ITagAssignment
from kendo.snowflake.utils.crud import (
    IInsert,
    ISelect,
    IInsertBulk,
    generate_insert,
    generate_insert_bulk,
    generate_select,
)


def create_tag(
    connection_name: str, name: str, allowed_values: Optional[List[str]] = None
):
    session = get_session(connection_name)

    # check for duplicate
    i_select_duplicate_check: ISelect = ISelect(
        table="kendo_db.config.tags", select=["id"], where=f"name = '{name}'"
    )

    dup_res = execute(session, generate_select(i_select_duplicate_check))
    if dup_res:
        print(f"Tag with name '{name}' already exists. Aborting...")
        raise typer.Abort()

    # INSERT into tags table
    declare_block = "res RESULTSET;"

    i_insert: IInsert = IInsert(table="kendo_db.config.tags", data={"name": name})
    insert_sql = generate_insert(i_insert)

    i_select: ISelect = ISelect(
        table="kendo_db.config.tags", select=["id"], where=f"name = '{name}'"
    )
    select_sql = """
        res := ({query});
    """.format(
        query=generate_select(i_select, include_semicolon=False)
    )

    return_block = """
        RETURN TABLE(res);
    """

    sql_statements = insert_sql + "\n" + select_sql + "\n" + return_block
    res = execute_anonymous_block(session, sql_statements, declare_block=declare_block)
    if res is None:
        print(f"Could not fetch. Aborting...")
        raise typer.Abort()

    # INSERT into tags_allowed_values table
    tag_id = res[0]["ID"]
    if allowed_values:
        i_insert_bulk: IInsertBulk = IInsertBulk(
            table="kendo_db.config.tags_allowed_values",
            data=[{"tag_id": tag_id, "value": value} for value in allowed_values],
        )
        execute(session, generate_insert_bulk(i_insert_bulk))

    print(f"Tag '{name}' created successfully.")

    close_session(session)


def show_tags(connection_name: str, name: Optional[str]):
    session = get_session(connection_name)
    i_select: ISelect = ISelect(
        table="kendo_db.config.tags",
    )
    if name:
        i_select.where = f"name LIKE '%{name}%'"

    res = execute(session, generate_select(i_select))
    if res and len(res) > 0:
        for i, _ in enumerate(res):
            tag_id = res[i]["ID"]
            i_select = ISelect(
                table="kendo_db.config.tags_allowed_values",
                select=["value"],
                where=f"tag_id = {tag_id}",
            )
            allowed_values = execute(session, generate_select(i_select))
            if allowed_values:
                res[i]["allowed_values"] = [val["VALUE"] for val in allowed_values]
            del res[i]["ID"]
    print_json(data=res)
    close_session(session)


def set_tag(connection_name: str, file_path: Path):
    # TODO: handle effect on existing policies

    if file_path.is_file():
        session = get_session(connection_name)

        tag_assignment_data = None
        with open(file_path) as f:
            tag_assignment_data = json.load(f)

        # validate data
        tag_assignment_data = ITagAssignment.model_validate(tag_assignment_data)

        # check if tag exists
        i_select: ISelect = ISelect(
            table="kendo_db.config.tags",
            select=["id"],
            where=f"name = '{tag_assignment_data.tag}'",
        )
        tag_res = execute(session, generate_select(i_select))
        if not tag_res:
            print(f"Tag with name '{tag_assignment_data.tag}' not found. Aborting...")
            raise typer.Abort()
        tag_id = tag_res[0]["ID"]

        # check if tag value is valid
        i_select = ISelect(
            table="kendo_db.config.tags_allowed_values",
            select=["value"],
            where=f"tag_id = {tag_id}",
        )
        allowed_values = execute(session, generate_select(i_select))
        if allowed_values and len(allowed_values) > 0:
            allowed_values = [val["VALUE"] for val in allowed_values]
            if tag_assignment_data.value not in allowed_values:
                print(
                    f"Tag value '{tag_assignment_data.value}' not allowed. Aborting..."
                )
                raise typer.Abort()

        # for each object
        for obj in tag_assignment_data.objects:
            #  TODO: check if object exists

            #  check if object has tag already
            i_select = ISelect(
                table="kendo_db.config.tags_assignments",
                select=["id"],
                where=f"tag_id = {str(tag_id)} \
                AND value = '{tag_assignment_data.value}' \
                AND obj_type = '{obj.type.value}' \
                AND obj_path = '{obj.path}'",
            )
            tag_assignment_check = execute(session, generate_select(i_select))
            if tag_assignment_check:
                print(
                    f"Tag '{tag_assignment_data.tag}' with value '{tag_assignment_data.value}' already assigned to {obj.type} '{obj.path}'. Skipping..."
                )
                continue
            else:
                #  if not, insert into tag_assignments
                i_insert: IInsert = IInsert(
                    table="kendo_db.config.tags_assignments",
                    data={
                        "obj_type": obj.type.value,
                        "obj_path": obj.path,
                        "tag_id": tag_id,
                        "value": tag_assignment_data.value,
                    },
                )
                execute(session, generate_insert(i_insert))

        print(f"Tag '{tag_assignment_data.tag}' set successfully.")
        close_session(session)
    else:
        print(f"File not found: {file_path}")
        raise typer.Abort()
