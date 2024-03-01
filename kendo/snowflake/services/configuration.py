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
from kendo.snowflake.schemas.common import ICaughtException
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
    colored_print("Scanning databases...", level="info")
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
    temp_list = [db["name"] for db in dbs_in_sf]
    for db in dbs_in_kendo:
        if db["NAME"] not in temp_list:
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
    temp_list = [db["NAME"] for db in dbs_in_kendo]
    for db in dbs_in_sf:
        if db["name"] not in temp_list:
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
    kendo_db_id_map = {db["ID"]: db["NAME"] for db in dbs_in_kendo}

    colored_print("Scanning schemas...", level="info")
    # fetch Schemas from SF
    # attach kendo's database_id to each schema (match by catalog_name)
    # fetch Schemas from Kendo
    # show missing and new (match by name and database_id)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed
    schemas_in_sf = []
    skipped_dbs = []
    for db in dbs_in_kendo:
        schemas_in_this_db = execute(
            session, f"show schemas in {db['NAME']}", abort_on_exception=False
        )
        if isinstance(schemas_in_this_db, ICaughtException):
            skipped_dbs.append(
                {
                    "obj": db["NAME"],
                    "type": "database",
                    "error": schemas_in_this_db.message,
                }
            )
            continue
        schemas_in_this_db = [
            {
                "name": schema["name"],
                "created_on": schema["created_on"],
                "database_id": db["ID"],
                "database_name": db["NAME"],
            }
            for schema in schemas_in_this_db
            if schema["name"].lower() not in exclusion_rules.get("schemas", [])
        ]
        schemas_in_sf.extend(schemas_in_this_db)
    if skipped_dbs:
        colored_print(
            "Schemas could not be scanned from some databases.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Skipped: ", level="info")
            print(skipped_dbs)
        typer.confirm(
            "Do you want to proceed with mapping excluding schemas from these databases?",
            abort=True,
        )
    schemas_in_kendo = execute(
        session,
        generate_select(
            ISelect(table="kendo_db.infrastructure.schema_objs"),
        ),
    )
    schemas_in_kendo = list(
        map(
            lambda schema: {
                **schema,
                "DATABASE_NAME": kendo_db_id_map[schema["DATABASE_ID"]],
            },
            schemas_in_kendo,
        )
    )

    missing_schemas = []
    temp_list = [(schema["name"], schema["database_id"]) for schema in schemas_in_sf]
    for schema in schemas_in_kendo:
        if (schema["NAME"], schema["DATABASE_ID"]) not in temp_list:
            missing_schemas.append(schema)
    if missing_schemas:
        colored_print(
            "Some schemas names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Schema mappings: ", level="info")
            print(missing_schemas)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_schemas = []
    temp_list = [(schema["NAME"], schema["DATABASE_ID"]) for schema in schemas_in_kendo]
    for schema in schemas_in_sf:
        if (schema["name"], schema["database_id"]) not in temp_list:
            new_schemas.append(schema)
    if new_schemas:
        colored_print("New schemas detected since last scan.", level="info")
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Schemas: ", level="info")
            print(new_schemas)
        typer.confirm(
            "Are you sure you want these new schemas names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new schemas...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.schema_objs",
                columns=["obj_created_on", "name", "database_id"],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", schema["created_on"]),
                    schema["name"],
                    schema["database_id"],
                )
                for schema in new_schemas
            ]
            execute_many(session, insert_statement, data)
        colored_print("New schemas mapped successfully.", level="success")
        schemas_in_kendo = execute(
            session,
            generate_select(
                ISelect(table="kendo_db.infrastructure.schema_objs"),
            ),
        )
        schemas_in_kendo = list(
            map(
                lambda schema: {
                    **schema,
                    "DATABASE_NAME": kendo_db_id_map[schema["DATABASE_ID"]],
                },
                schemas_in_kendo,
            )
        )
    kendo_schema_id_map = {
        schema["ID"]: {"name": schema["NAME"], "database_id": schema["DATABASE_ID"]}
        for schema in schemas_in_kendo
    }

    colored_print("Scanning tables...", level="info")
    # fetch Tables from SF
    # attach kendo's schema_id to each Table (match by table_schema, table_catalog)
    # fetch Tables from Kendo
    # show missing and new (match by name and schema_id)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed
    tables_in_sf = []
    skipped_schemas = []
    for schema in schemas_in_kendo:
        tables_in_this_schema = execute(
            session,
            f"show tables in {schema['DATABASE_NAME']}.{schema['NAME']}",
            abort_on_exception=False,
        )
        if isinstance(tables_in_this_schema, ICaughtException):
            skipped_schemas.append(
                {
                    "obj": f"{schema['DATABASE_NAME']}.{schema['NAME']}",
                    "type": "schema",
                    "error": tables_in_this_schema.message,
                }
            )
            continue
        tables_in_this_schema = [
            {
                "name": table["name"],
                "created_on": table["created_on"],
                "schema_id": schema["ID"],
                "schema_name": schema["NAME"],
                "database_id": schema["DATABASE_ID"],
                "database_name": schema["DATABASE_NAME"],
            }
            for table in tables_in_this_schema
        ]
        tables_in_sf.extend(tables_in_this_schema)
    if skipped_schemas:
        colored_print(
            "Tables could not be scanned from some schemas.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Skipped: ", level="info")
            print(skipped_schemas)
        typer.confirm(
            "Do you want to proceed with mapping excluding tables from these schemas?",
            abort=True,
        )
    tables_in_kendo = execute(
        session,
        generate_select(
            ISelect(table="kendo_db.infrastructure.table_objs"),
        ),
    )
    tables_in_kendo = list(
        map(
            lambda table: {
                **table,
                "SCHEMA_NAME": kendo_schema_id_map[table["SCHEMA_ID"]]["name"],
                "DATABASE_ID": kendo_schema_id_map[table["SCHEMA_ID"]]["database_id"],
                "DATABASE_NAME": kendo_db_id_map[
                    kendo_schema_id_map[table["SCHEMA_ID"]]["database_id"]
                ],
            },
            tables_in_kendo,
        ),
    )

    missing_tables = []
    temp_list = [(table["name"], table["schema_id"]) for table in tables_in_sf]
    for table in tables_in_kendo:
        if (table["NAME"], table["SCHEMA_ID"]) not in temp_list:
            missing_tables.append(table)
    if missing_tables:
        colored_print(
            "Some tables names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Table mappings: ", level="info")
            print(missing_tables)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_tables = []
    temp_list = [(table["NAME"], table["SCHEMA_ID"]) for table in tables_in_kendo]
    for table in tables_in_sf:
        if (table["name"], table["schema_id"]) not in temp_list:
            new_tables.append(table)
    if new_tables:
        colored_print("New tables detected since last scan.", level="info")
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Tables: ", level="info")
            print(new_tables)
        typer.confirm(
            "Are you sure you want these new tables names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new tables...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.table_objs",
                columns=["obj_created_on", "name", "schema_id"],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", table["created_on"]),
                    table["name"],
                    table["schema_id"],
                )
                for table in new_tables
            ]
            execute_many(session, insert_statement, data)
        colored_print("New tables mapped successfully.", level="success")
        tables_in_kendo = execute(
            session,
            generate_select(
                ISelect(table="kendo_db.infrastructure.table_objs"),
            ),
        )
        tables_in_kendo = list(
            map(
                lambda table: {
                    **table,
                    "SCHEMA_NAME": kendo_schema_id_map[table["SCHEMA_ID"]]["name"],
                    "DATABASE_ID": kendo_schema_id_map[table["SCHEMA_ID"]][
                        "database_id"
                    ],
                    "DATABASE_NAME": kendo_db_id_map[
                        kendo_schema_id_map[table["SCHEMA_ID"]]["database_id"]
                    ],
                },
                tables_in_kendo,
            ),
        )
    kendo_table_id_map = {
        table["ID"]: {
            "name": table["NAME"],
            "schema_id": table["SCHEMA_ID"],
            "database_id": table["DATABASE_ID"],
        }
        for table in tables_in_kendo
    }

    colored_print("Scanning columns...", level="info")
    # fetch Columns from SF
    # attach kendo's table_id to each Column (match by table_name, table_schema, table_catalog)
    # fetch Columns from Kendo
    # show missing and new (match by name and table_id)
    # prompt to record the new ones
    # insert the new records
    # select all records and store in memory, id will be needed
    columns_in_sf = []
    skipped_tables = []
    for table in tables_in_kendo:
        columns_in_this_table = execute(
            session,
            f"show columns in {table['DATABASE_NAME']}.{table['SCHEMA_NAME']}.{table['NAME']}",
            abort_on_exception=False,
        )
        if isinstance(columns_in_this_table, ICaughtException):
            skipped_tables.append(
                {
                    "obj": f"{table['DATABASE_NAME']}.{table['SCHEMA_NAME']}.{table['NAME']}",
                    "type": "table",
                    "error": columns_in_this_table.message,
                }
            )
            continue
        columns_in_this_table = [
            {
                "name": column["column_name"],
                "created_on": None,
                "table_id": table["ID"],
                "table_name": table["NAME"],
                "schema_id": table["SCHEMA_ID"],
                "schema_name": table["SCHEMA_NAME"],
                "database_id": table["DATABASE_ID"],
                "database_name": table["DATABASE_NAME"],
            }
            for column in columns_in_this_table
        ]
        columns_in_sf.extend(columns_in_this_table)
    if skipped_tables:
        colored_print(
            "Columns could not be scanned from some tables.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Skipped: ", level="info")
            print(skipped_tables)
        typer.confirm(
            "Do you want to proceed with mapping excluding columns from these tables?",
            abort=True,
        )
    columns_in_kendo = execute(
        session,
        generate_select(
            ISelect(table="kendo_db.infrastructure.column_objs"),
        ),
    )
    columns_in_kendo = list(
        map(
            lambda column: {
                **column,
                "TABLE_NAME": kendo_table_id_map[column["TABLE_ID"]]["name"],
                "SCHEMA_ID": kendo_table_id_map[column["TABLE_ID"]]["schema_id"],
                "SCHEMA_NAME": kendo_schema_id_map[
                    kendo_table_id_map[column["TABLE_ID"]]["schema_id"]
                ]["name"],
                "DATABASE_ID": kendo_table_id_map[column["TABLE_ID"]]["database_id"],
                "DATABASE_NAME": kendo_db_id_map[
                    kendo_table_id_map[column["TABLE_ID"]]["database_id"]
                ],
            },
            columns_in_kendo,
        ),
    )

    missing_columns = []
    temp_list = [(column["name"], column["table_id"]) for column in columns_in_sf]
    for column in columns_in_kendo:
        if (column["NAME"], column["TABLE_ID"]) not in temp_list:
            missing_columns.append(column)
    if missing_columns:
        colored_print(
            "Some columns names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Column mappings: ", level="info")
            print(missing_columns)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_columns = []
    temp_list = [(column["NAME"], column["TABLE_ID"]) for column in columns_in_kendo]
    for column in columns_in_sf:
        if (column["name"], column["table_id"]) not in temp_list:
            new_columns.append(column)
    if new_columns:
        colored_print("New columns detected since last scan.", level="info")
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Columns: ", level="info")
            print(new_columns)
        typer.confirm(
            "Are you sure you want these new columns names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new columns...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.column_objs",
                columns=["name", "table_id"],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    column["name"],
                    column["table_id"],
                )
                for column in new_columns
            ]
            execute_many(session, insert_statement, data)
        colored_print("New columns mapped successfully.", level="success")
        columns_in_kendo = execute(
            session,
            generate_select(
                ISelect(table="kendo_db.infrastructure.column_objs"),
            ),
        )
        columns_in_kendo = list(
            map(
                lambda column: {
                    **column,
                    "TABLE_NAME": kendo_table_id_map[column["TABLE_ID"]]["name"],
                    "SCHEMA_ID": kendo_table_id_map[column["TABLE_ID"]]["schema_id"],
                    "SCHEMA_NAME": kendo_schema_id_map[
                        kendo_table_id_map[column["TABLE_ID"]]["schema_id"]
                    ]["name"],
                    "DATABASE_ID": kendo_table_id_map[column["TABLE_ID"]][
                        "database_id"
                    ],
                    "DATABASE_NAME": kendo_db_id_map[
                        kendo_table_id_map[column["TABLE_ID"]]["database_id"]
                    ],
                },
                columns_in_kendo,
            ),
        )

    close_session(session)
