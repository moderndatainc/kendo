import typer
import snowflake.connector
import tomli
import os
from snowflake.connector import connect, DictCursor
from snowflake.connector.errors import ProgrammingError

from kendo.snowflake.schemas.common import ICaughtException

snowflake.connector.paramstyle = "qmark"


def get_snowflake_session(connection_name="default"):
    return connect(connection_name=connection_name)

def get_credentials_from_toml(connection_name="default"):
    snowflake_local_dir = os.path.join(os.path.expanduser("~"), ".snowflake")
    connections_path = os.path.join(snowflake_local_dir, "connections.toml")
    with open(connections_path, "rb") as f:
        config = tomli.load(f)
    return config[connection_name]

def execute(
    session,
    sql,
    parameters=None,
    use_role=None,
    use_warehouse=None,
    print_sql=False,
    abort_on_exception=True,
):
    with session.cursor(DictCursor) as cur:
        try:
            if use_role:
                cur.execute(f"USE ROLE {use_role}")
            if use_warehouse:
                cur.execute(f"USE WAREHOUSE {use_warehouse}")

            if print_sql:
                print("--------------------")
                print(sql)
                print("--------------------")
            res = cur.execute(sql, parameters).fetchall()
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            if abort_on_exception:
                print(e)
                raise typer.Abort()
            else:
                return ICaughtException(message=str(e))


def execute_anonymous_block(
    session,
    sql_body,
    declare_block=None,
    use_role=None,
    use_warehouse=None,
    print_sql=False,
):
    with session.cursor(DictCursor) as cur:
        try:
            if use_role:
                cur.execute(f"USE ROLE {use_role}")
            if use_warehouse:
                cur.execute(f"USE WAREHOUSE {use_warehouse}")
            sql = """
                EXECUTE IMMEDIATE $$
                """
            if declare_block:
                sql += (
                    """
                    DECLARE
                    """
                    + declare_block
                )
            sql += (
                """
                BEGIN
                """
                + sql_body
            )
            sql += """
                END;
                $$;
                """
            if print_sql:
                print("--------------------")
                print(sql)
                print("--------------------")
            res = cur.execute(sql).fetchall()
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            print(e)
            raise typer.Abort()


def execute_many(
    session,
    sql,
    seq_of_parameters=None,
    use_role=None,
    use_warehouse=None,
    print_sql=False,
):
    # For batch inserts
    with session.cursor(DictCursor) as cur:
        try:
            if use_role:
                cur.execute(f"USE ROLE {use_role}")
            if use_warehouse:
                cur.execute(f"USE WAREHOUSE {use_warehouse}")

            if print_sql:
                print("--------------------")
                print(sql)
                print("--------------------")
            res = cur.executemany(sql, seq_of_parameters).fetchall()
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            print(e)
            raise typer.Abort()


def close_snowflake_session(session):
    session.close()
