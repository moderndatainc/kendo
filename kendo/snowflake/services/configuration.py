from rich import print
from kendo.snowflake.connection import (
    get_session,
    close_session,
    execute_anonymous_block,
)
from kendo.snowflake.utils.constants import ANONYMOUS_BLOCK, COMPLETED
from kendo.snowflake.ddl.config.v1 import SQL as config_v1_sql


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
