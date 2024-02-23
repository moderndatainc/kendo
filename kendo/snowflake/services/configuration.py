import typer
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn
from kendo.snowflake.connection import (
    get_session,
    close_session,
    execute_anonymous_block,
    execute,
    execute_many,
)
from kendo.snowflake.utils.constants import ANONYMOUS_BLOCK, COMPLETED
from kendo.snowflake.ddl.config.v1 import SQL as config_v1_sql
from kendo.snowflake.utils.crud import (
    IInsertV2,
    ISelect,
    generate_insert_v2,
    generate_select,
)
from kendo.snowflake.utils.rich import colored_print

exclusion_rules = {
    "databases": ["snowflake", "snowflake_sample_data", "kendo_db"],
    "schemas": ["information_schema"],
}


def setup_config_database(connection_name: str):
    session = get_session(connection_name)

    # body
    sql_statments = """
        USE ROLE {role};
    """.format(
        role=session.role,
    )
    sql_statments += config_v1_sql
    sql_statments += """
        RETURN '{result}';
    """.format(
        result=COMPLETED,
    )

    res = execute_anonymous_block(
        session, sql_statments, use_warehouse=session.warehouse
    )

    if res is not None and res[0][ANONYMOUS_BLOCK] == COMPLETED:
        print("Setup completed successfully")

    close_session(session)


def scan_infra(connection_name: str):
    session = get_session(connection_name)

    colored_print("Scanning Snowflake infrastructure...", level="info")
    colored_print("Mapping databases...", level="info")
    # fetch Databases from SF
    # fetch Databases from Kendo
    # show missing and new (match by name)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed
    dbs_in_sf = execute(session, f"show databases")
    dbs_in_sf = [
        {"name": db["name"], "created_on": db["created_on"]}
        for db in dbs_in_sf
        if db["name"].lower() not in exclusion_rules.get("databases", [])
    ]
    dbs_in_kendo = execute(
        session,
        generate_select(ISelect(table="kendo_db.infrastructure.database_objs")),
    )

    missing_dbs = []
    for db in dbs_in_kendo:
        if db["NAME"] not in [db["name"] for db in dbs_in_sf]:
            missing_dbs.append(db)
    if missing_dbs:
        colored_print(
            "Some databases names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Database mappings: ", level="info")
            print(missing_dbs)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_dbs = []
    for db in dbs_in_sf:
        if db["name"] not in [db["NAME"] for db in dbs_in_kendo]:
            new_dbs.append(db)
    if new_dbs:
        colored_print("New databases detected since last scan.", level="info")
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Databases: ", level="info")
            print(new_dbs)
        typer.confirm(
            "Are you sure you want these new databases names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new databases...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.database_objs",
                columns=["obj_created_on", "name"],
            )
            insert_statement = generate_insert_v2(i_insert)
            # db["created_on"].strftime("%Y-%m-%d %H:%M:%S.%f")
            data = [(("TIMESTAMP_LTZ", db["created_on"]), db["name"]) for db in new_dbs]
            execute_many(session, insert_statement, data)
        colored_print("New databases mapped successfully.", level="success")
    dbs_in_kendo = execute(
        session,
        generate_select(ISelect(table="kendo_db.infrastructure.database_objs")),
    )

    # fetch Schemas from SF
    # # execute(session, f"show schemas in {db['name']}")
    # attach kendo's database_id to each schema (match by catalog_name)
    # fetch Schemas from Kendo
    # show missing and new (match by name and database_id)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed

    # fetch Tables from SF
    # # execute(session, f"show tables in {db['name']}.{schema['name']}")
    # attach kendo's schema_id to each Table (match by table_schema, table_catalog)
    # fetch Tables from Kendo
    # show missing and new (match by name and schema_id)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed

    # fetch Columns from SF
    # attach kendo's table_id to each Column (match by table_name, table_schema, table_catalog)
    # fetch Columns from Kendo
    # show missing and new (match by name and table_id)
    # prompt to record the new ones
    # insert the new records

    close_session(session)
