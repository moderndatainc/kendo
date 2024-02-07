from typing import List, Optional
from rich import print
import typer
from kendo.snowflake.connection import (
    get_session,
    close_session,
    execute_anonymous_block,
    execute,
)
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
