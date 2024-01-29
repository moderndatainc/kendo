from snowflake.connector import connect, DictCursor
from snowflake.connector.errors import ProgrammingError


def get_session(connection_name="default"):
    return connect(connection_name=connection_name)


def execute(session, sql, use_role=None, use_warehouse=None):
    with session.cursor(DictCursor) as cur:
        try:
            if use_role:
                print(
                    f"[{session.warehouse}:{session.user}:{session.role}] >>>",
                    f"USE ROLE {use_role}",
                )
                cur.execute(f"USE ROLE {use_role}")
            if use_warehouse:
                print(
                    f"[{session.warehouse}:{session.user}:{session.role}] >>>",
                    f"USE WAREHOUSE {use_warehouse}",
                )
                cur.execute(f"USE WAREHOUSE {use_warehouse}")

            res = cur.execute(sql).fetchall()  # type: ignore
            # print(json.dumps(res, indent=4, sort_keys=True, default=str))
            return res
        except ProgrammingError as e:
            print(e)


def close_session(session):
    session.close()
