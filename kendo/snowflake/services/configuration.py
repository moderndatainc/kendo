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
    "roles": [
        "orgadmin",
        "accountadmin",
        "securityadmin",
        "sysadmin",
        "useradmin",
        "kendoadmin",
        "public",
    ],
}


def setup_config_database(connection_name: str):
    session = get_session(connection_name)

    # body
    sql_statments = """
        USE ROLE {role};
    """.format(
        role="SYSADMIN",
    )
    sql_statments += config_v1_sql
    sql_statments += """
        RETURN '{result}';
    """.format(
        result=COMPLETED,
    )

    res = execute_anonymous_block(
        session, sql_statments
    )

    if res is not None and res[0][ANONYMOUS_BLOCK] == COMPLETED:
        print("Setup completed successfully")

    close_session(session)


def scan_infra(connection_name: str):
    session = get_session(connection_name)

    # check if user has SYSADMIN
    role_grant_check = execute(session, f"SHOW GRANTS TO USER {session.user}")
    ROLE_SYSADMIN = ""
    ROLE_SECURITYADMIN = ""
    for i, grant in enumerate(role_grant_check):
        if grant["role"] == "SYSADMIN":
            ROLE_SYSADMIN = "SYSADMIN"
        if grant["role"] == "SECURITYADMIN":
            ROLE_SECURITYADMIN = "SECURITYADMIN"
    if not ROLE_SYSADMIN:
        print("User does not have SYSADMIN role. Aborting...")
        raise typer.Abort()
    if not ROLE_SECURITYADMIN:
        print("User does not have SECURITYADMIN role. Aborting...")
        raise typer.Abort()

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
            f"{len(missing_dbs)} database(s) that were mapped earlier could not be found.",
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
        colored_print(
            f"{len(new_dbs)} new database(s) detected since last scan.",
            level="info",
        )
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
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_dbs)} new database(s) mapped successfully.", level="success"
        )
        dbs_in_kendo = execute(
            session,
            generate_select(ISelect(table="kendo_db.infrastructure.database_objs")),
        )
    kendo_db_id_key_map = {db["ID"]: db["NAME"] for db in dbs_in_kendo}

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
                "DATABASE_NAME": kendo_db_id_key_map[schema["DATABASE_ID"]],
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
            f"{len(missing_schemas)} schema(s) that were mapped earlier could not be found.",
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
        colored_print(
            f"{len(new_schemas)} new schema(s) detected since last scan.", level="info"
        )
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
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_schemas)} new schema(s) mapped successfully.", level="success"
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
                    "DATABASE_NAME": kendo_db_id_key_map[schema["DATABASE_ID"]],
                },
                schemas_in_kendo,
            )
        )
    kendo_schema_id_key_map = {
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
                "SCHEMA_NAME": kendo_schema_id_key_map[table["SCHEMA_ID"]]["name"],
                "DATABASE_ID": kendo_schema_id_key_map[table["SCHEMA_ID"]][
                    "database_id"
                ],
                "DATABASE_NAME": kendo_db_id_key_map[
                    kendo_schema_id_key_map[table["SCHEMA_ID"]]["database_id"]
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
            f"{len(missing_tables)} table(s) that were mapped earlier could not be found.",
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
        colored_print(
            f"{len(new_tables)} new table(s) detected since last scan.", level="info"
        )
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
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_tables)} new table(s) mapped successfully.", level="success"
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
                    "SCHEMA_NAME": kendo_schema_id_key_map[table["SCHEMA_ID"]]["name"],
                    "DATABASE_ID": kendo_schema_id_key_map[table["SCHEMA_ID"]][
                        "database_id"
                    ],
                    "DATABASE_NAME": kendo_db_id_key_map[
                        kendo_schema_id_key_map[table["SCHEMA_ID"]]["database_id"]
                    ],
                },
                tables_in_kendo,
            ),
        )
    kendo_table_id_key_map = {
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
                "TABLE_NAME": kendo_table_id_key_map[column["TABLE_ID"]]["name"],
                "SCHEMA_ID": kendo_table_id_key_map[column["TABLE_ID"]]["schema_id"],
                "SCHEMA_NAME": kendo_schema_id_key_map[
                    kendo_table_id_key_map[column["TABLE_ID"]]["schema_id"]
                ]["name"],
                "DATABASE_ID": kendo_table_id_key_map[column["TABLE_ID"]][
                    "database_id"
                ],
                "DATABASE_NAME": kendo_db_id_key_map[
                    kendo_table_id_key_map[column["TABLE_ID"]]["database_id"]
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
            f"{len(missing_columns)} column(s) that were mapped earlier could not be found.",
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
        colored_print(
            f"{len(new_columns)} new column(s) detected since last scan.", level="info"
        )
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
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_columns)} new column(s) mapped successfully.", level="success"
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
                    "TABLE_NAME": kendo_table_id_key_map[column["TABLE_ID"]]["name"],
                    "SCHEMA_ID": kendo_table_id_key_map[column["TABLE_ID"]][
                        "schema_id"
                    ],
                    "SCHEMA_NAME": kendo_schema_id_key_map[
                        kendo_table_id_key_map[column["TABLE_ID"]]["schema_id"]
                    ]["name"],
                    "DATABASE_ID": kendo_table_id_key_map[column["TABLE_ID"]][
                        "database_id"
                    ],
                    "DATABASE_NAME": kendo_db_id_key_map[
                        kendo_table_id_key_map[column["TABLE_ID"]]["database_id"]
                    ],
                },
                columns_in_kendo,
            ),
        )

    colored_print("Scanning roles...", level="info")
    # fetch Roles from SF
    # fetch Roles from Kendo
    # show missing and new (match by name)
    # prompt to record the new ones
    # insert the new records
    roles_in_sf = execute(session, f"show roles")
    roles_in_sf = [
        {"name": role["name"], "created_on": role["created_on"]} for role in roles_in_sf
    ]
    roles_in_kendo = execute(
        session,
        generate_select(ISelect(table="kendo_db.infrastructure.role_objs")),
    )

    missing_roles = []
    temp_list = [role["name"] for role in roles_in_sf]
    for role in roles_in_kendo:
        if role["NAME"] not in temp_list:
            missing_roles.append(role)
    if missing_roles:
        colored_print(
            f"{len(missing_roles)} roles names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Role mappings: ", level="info")
            print(missing_roles)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_roles = []
    temp_list = [role["NAME"] for role in roles_in_kendo]
    for role in roles_in_sf:
        if role["name"] not in temp_list:
            new_roles.append(role)
    if new_roles:
        colored_print(
            f"{len(new_roles)} new roles detected since last scan.", level="info"
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Roles: ", level="info")
            print(new_roles)
        typer.confirm(
            "Are you sure you want these new roles names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new roles...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.role_objs",
                columns=["obj_created_on", "name"],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", role["created_on"]),
                    role["name"],
                )
                for role in new_roles
            ]
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_roles)} new roles mapped successfully.", level="success"
        )
        roles_in_kendo = execute(
            session,
            generate_select(ISelect(table="kendo_db.infrastructure.role_objs")),
        )
    kendo_role_id_key_map = {role["ID"]: role["NAME"] for role in roles_in_kendo}
    kendo_role_name_key_map = {role["NAME"]: role["ID"] for role in roles_in_kendo}

    colored_print("Scanning users...", level="info")
    # fetch Users from SF
    # fetch Users from Kendo
    # show missing and new (match by name)
    # prompt to record the new ones
    users_in_sf = execute(session, f"show users", use_role=ROLE_SECURITYADMIN)
    # switch back to SYSADMIN role
    execute(session, f"USE ROLE {ROLE_SYSADMIN}")
    users_in_sf = [
        {
            "login_name": user["login_name"],
            "created_on": user["created_on"],
            "last_success_login": user["last_success_login"],
            "email": user["email"],
            "owner": user["owner"],
            "owner_role_id": (
                kendo_role_name_key_map[user["owner"]] if user["owner"] else None
            ),
            "default_role": user["default_role"],
            "default_role_id": (
                kendo_role_name_key_map[user["default_role"]]
                if user["default_role"]
                else None
            ),
            "ext_authn_uid": user["ext_authn_uid"],
            "is_ext_authn_duo": (
                False
                if (not user["ext_authn_uid"] or user["ext_authn_uid"] == "false")
                else True
            ),
        }
        for user in users_in_sf
    ]
    users_in_kendo = execute(
        session,
        generate_select(ISelect(table="kendo_db.infrastructure.user_objs")),
    )
    missing_users = []
    temp_list = [user["login_name"] for user in users_in_sf]
    for user in users_in_kendo:
        if user["LOGIN_NAME"] not in temp_list:
            missing_users.append(user)
    if missing_users:
        colored_print(
            f"{len(missing_users)} users names that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing User mappings: ", level="info")
            print(missing_users)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_users = []
    temp_list = [user["LOGIN_NAME"] for user in users_in_kendo]
    for user in users_in_sf:
        if user["login_name"] not in temp_list:
            new_users.append(user)
    if new_users:
        colored_print(
            f"{len(new_users)} new users detected since last scan.", level="info"
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Users: ", level="info")
            print(new_users)
        typer.confirm(
            "Are you sure you want these new users names to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new users...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.user_objs",
                columns=[
                    "obj_created_on",
                    "login_name",
                    "last_success_login",
                    "email",
                    "owner_role_id",
                    "default_role_id",
                    "ext_authn_uid",
                    "is_ext_authn_duo",
                ],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", user["created_on"]),
                    user["login_name"],
                    ("TIMESTAMP_LTZ", user["last_success_login"]),
                    user["email"],
                    user["owner_role_id"],
                    user["default_role_id"],
                    user["ext_authn_uid"],
                    user["is_ext_authn_duo"],
                )
                for user in new_users
            ]
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_users)} new users mapped successfully.", level="success"
        )
        users_in_kendo = execute(
            session,
            generate_select(ISelect(table="kendo_db.infrastructure.user_objs")),
        )
    kendo_user_id_key_map = {user["ID"]: user["LOGIN_NAME"] for user in users_in_kendo}
    kendo_user_name_key_map = {
        user["LOGIN_NAME"]: user["ID"] for user in users_in_kendo
    }

    colored_print("Scanning privilege grants to roles...", level="info")
    # create new map for database, schema, table with name as key
    # fetch grants from sf
    # print the object types on which grants were skipped
    # fetch grants from kendo
    # show missing and new
    # prompt to record the new ones
    # insert the new records
    kendo_db_name_key_map = {db["NAME"]: db["ID"] for db in dbs_in_kendo}
    kendo_schema_name_key_map = {
        schema["DATABASE_NAME"] + "." + schema["NAME"]: schema["ID"]
        for schema in schemas_in_kendo
    }
    kendo_schema_id_key_full_name_map = {
        schema["ID"]: schema["DATABASE_NAME"] + "." + schema["NAME"]
        for schema in schemas_in_kendo
    }
    kendo_table_name_key_map = {
        table["DATABASE_NAME"]
        + "."
        + table["SCHEMA_NAME"]
        + "."
        + table["NAME"]: table["ID"]
        for table in tables_in_kendo
    }
    kendo_table_id_key_full_name_map = {
        table["ID"]: table["DATABASE_NAME"]
        + "."
        + table["SCHEMA_NAME"]
        + "."
        + table["NAME"]
        for table in tables_in_kendo
    }
    kendo_all_obj_name_key_map = {
        "DATABASE": kendo_db_name_key_map,
        "SCHEMA": kendo_schema_name_key_map,
        "TABLE": kendo_table_name_key_map,
        "ROLE": kendo_role_name_key_map,
        "USER": kendo_user_name_key_map,
    }
    kendo_all_obj_id_key_map = {
        "DATABASE": kendo_db_id_key_map,
        "SCHEMA": kendo_schema_id_key_full_name_map,
        "TABLE": kendo_table_id_key_full_name_map,
        "ROLE": kendo_role_id_key_map,
        "USER": kendo_user_id_key_map,
    }
    privilege_grants_in_sf = []
    skipped_privilege_grants_on = set()
    for role in roles_in_kendo:
        if role["NAME"].lower() in exclusion_rules.get("roles", []):
            # skipping grants to internal roles
            continue

        grants_of_this_role = execute(
            session,
            f"show grants to role {role['NAME']}",
        )
        temp_list = []
        for grant in grants_of_this_role:
            if grant["granted_on"] in ["DATABASE", "SCHEMA", "TABLE"]:
                if (
                    grant["granted_on"] == "DATABASE"
                    and grant["name"] in exclusion_rules.get("databases", [])
                ) or (
                    grant["granted_on"] == "SCHEMA"
                    and grant["name"] in exclusion_rules.get("schemas", [])
                ):
                    # skipping grants on excluded objects
                    continue
                temp_list.append(
                    {
                        "created_on": grant["created_on"],
                        "privilege": grant["privilege"],
                        "granted_on": grant["granted_on"],
                        "granted_on_id": kendo_all_obj_name_key_map[
                            grant["granted_on"]
                        ][grant["name"]],
                        "granted_on_name": grant["name"],
                        "granted_to": "ROLE",
                        "granted_to_id": role["ID"],
                        "granted_to_name": role["NAME"],
                        "grant_option": grant["grant_option"],
                    }
                )
            else:
                skipped_privilege_grants_on.add(grant["granted_on"])
        privilege_grants_in_sf.extend(temp_list)
    if len(skipped_privilege_grants_on) > 0:
        colored_print(
            "Privilege grants on the following types of objects were skipped.",
            level="warning",
        )
        print(skipped_privilege_grants_on)
    privilege_grants_in_kendo = execute(
        session,
        generate_select(
            ISelect(table="kendo_db.infrastructure.grants_privilege_objs"),
        ),
    )
    privilege_grants_in_kendo = list(
        map(
            lambda grant: {
                **grant,
                "GRANTED_ON_NAME": kendo_all_obj_id_key_map[grant["GRANTED_ON"]][
                    grant["GRANTED_ON_ID"]
                ],
                "GRANTED_TO_NAME": kendo_role_id_key_map[grant["GRANTED_TO_ID"]],
            },
            privilege_grants_in_kendo,
        ),
    )
    missing_grants = []
    # matching by booleans like grant["grant_option"] doesn't work
    temp_list = [
        (
            grant["privilege"],
            grant["granted_on"],
            grant["granted_on_id"],
            grant["granted_to"],
            grant["granted_to_id"],
        )
        for grant in privilege_grants_in_sf
    ]
    for grant in privilege_grants_in_kendo:
        if (
            grant["PRIVILEGE"],
            grant["GRANTED_ON"],
            grant["GRANTED_ON_ID"],
            grant["GRANTED_TO"],
            grant["GRANTED_TO_ID"],
        ) not in temp_list:
            missing_grants.append(grant)
    if missing_grants:
        colored_print(
            f"{len(missing_grants)} privilege grant(s) that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Privilege Grant mappings: ", level="info")
            print(missing_grants)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_grants = []
    temp_list = [
        (
            grant["PRIVILEGE"],
            grant["GRANTED_ON"],
            grant["GRANTED_ON_ID"],
            grant["GRANTED_TO"],
            grant["GRANTED_TO_ID"],
        )
        for grant in privilege_grants_in_kendo
    ]
    for grant in privilege_grants_in_sf:
        if (
            grant["privilege"],
            grant["granted_on"],
            grant["granted_on_id"],
            grant["granted_to"],
            grant["granted_to_id"],
        ) not in temp_list:
            new_grants.append(grant)
    if new_grants:
        colored_print(
            f"{len(new_grants)} new privilege grant(s) detected since last scan.",
            level="info",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Grants: ", level="info")
            print(new_grants)
        typer.confirm(
            "Are you sure you want these new privilege grants to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new privilege grants...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.grants_privilege_objs",
                columns=[
                    "obj_created_on",
                    "privilege",
                    "granted_on",
                    "granted_on_id",
                    "granted_to",
                    "granted_to_id",
                    "grant_option",
                ],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", grant["created_on"]),
                    grant["privilege"],
                    grant["granted_on"],
                    grant["granted_on_id"],
                    grant["granted_to"],
                    grant["granted_to_id"],
                    grant["grant_option"],
                )
                for grant in new_grants
            ]
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_grants)} new privilege grant(s) mapped successfully.",
            level="success",
        )

    colored_print("Scanning role grants...", level="info")
    # fetch grants from sf
    # fetch grants from kendo
    # show missing and new
    # prompt to record the new ones
    role_grants_in_sf = []
    for role in roles_in_kendo:
        grants_of_this_role = execute(
            session,
            f"show grants of role {role['NAME']}",
        )
        grants_of_this_role = [
            {
                "created_on": grant["created_on"],
                "role": grant["role"],
                "role_id": kendo_role_name_key_map[grant["role"]],
                "granted_to": grant["granted_to"],
                "grantee_name": grant["grantee_name"],
                "granted_to_id": kendo_all_obj_name_key_map[grant["granted_to"]][
                    grant["grantee_name"]
                ],
                "granted_by": grant["granted_by"],
                "granted_by_role_id": (
                    kendo_role_name_key_map[grant["granted_by"]]
                    if grant["granted_by"]
                    else None
                ),
            }
            for grant in grants_of_this_role
        ]
        role_grants_in_sf.extend(grants_of_this_role)
    role_grants_in_kendo = execute(
        session,
        generate_select(
            ISelect(table="kendo_db.infrastructure.grants_role_objs"),
        ),
    )
    role_grants_in_kendo = list(
        map(
            lambda grant: {
                **grant,
                "ROLE": kendo_role_id_key_map[grant["ROLE_ID"]],
                "GRANTEE_NAME": kendo_all_obj_id_key_map[grant["GRANTED_TO"]][
                    grant["GRANTED_TO_ID"]
                ],
                "GRANTED_BY": (
                    kendo_role_id_key_map[grant["GRANTED_BY_ROLE_ID"]]
                    if grant["GRANTED_BY_ROLE_ID"]
                    else None
                ),
            },
            role_grants_in_kendo,
        ),
    )
    missing_role_grants = []
    temp_list = [
        (
            grant["role"],
            grant["granted_to"],
            grant["granted_to_id"],
        )
        for grant in role_grants_in_sf
    ]
    for grant in role_grants_in_kendo:
        if (
            grant["ROLE"],
            grant["GRANTED_TO"],
            grant["GRANTED_TO_ID"],
        ) not in temp_list:
            missing_role_grants.append(grant)
    if missing_role_grants:
        colored_print(
            f"{len(missing_role_grants)} role grant(s) that were mapped earlier could not be found.",
            level="warning",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("Missing Role Grant mappings: ", level="info")
            print(missing_role_grants)
        typer.confirm(
            "Do you want to proceed without fixing these mappings yourself?",
            abort=True,
        )

    new_role_grants = []
    temp_list = [
        (
            grant["ROLE"],
            grant["GRANTED_TO"],
            grant["GRANTED_TO_ID"],
        )
        for grant in role_grants_in_kendo
    ]
    for grant in role_grants_in_sf:
        if (
            grant["role"],
            grant["granted_to"],
            grant["granted_to_id"],
        ) not in temp_list:
            new_role_grants.append(grant)
    if new_role_grants:
        colored_print(
            f"{len(new_role_grants)} new role grant(s) detected since last scan.",
            level="info",
        )
        confirm = typer.confirm("View?")
        if confirm:
            colored_print("New Role Grants: ", level="info")
            print(new_role_grants)
        typer.confirm(
            "Are you sure you want these new role grants to be mapped?",
            abort=True,
        )
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            # transient=True,
        ) as progress:
            progress.add_task(description="Mapping new role grants...", total=None)
            i_insert = IInsertV2(
                table="kendo_db.infrastructure.grants_role_objs",
                columns=[
                    "obj_created_on",
                    "role_id",
                    "granted_to",
                    "granted_to_id",
                    "granted_by_role_id",
                ],
            )
            insert_statement = generate_insert_v2(i_insert)
            data = [
                (
                    ("TIMESTAMP_LTZ", grant["created_on"]),
                    grant["role_id"],
                    grant["granted_to"],
                    grant["granted_to_id"],
                    grant["granted_by_role_id"],
                )
                for grant in new_role_grants
            ]
            execute_many(session, insert_statement, data, use_role=ROLE_SYSADMIN)
        colored_print(
            f"{len(new_role_grants)} new role grant(s) mapped successfully.",
            level="success",
        )

    close_session(session)
