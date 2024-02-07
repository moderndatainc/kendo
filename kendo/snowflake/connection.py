from snowflake.connector import connect, DictCursor
from snowflake.connector.errors import ProgrammingError


def get_session(connection_name="default"):
    return connect(connection_name=connection_name)


def execute(session, sql, use_role=None, use_warehouse=None, print_sql=False):
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
            res = cur.execute(sql).fetchall()  # type: ignore
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            print(e)


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
            res = cur.execute(sql).fetchall()  # type: ignore
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            print(e)


def close_session(session):
    session.close()
