import typer
import snowflake.connector
from typing import Any
from snowflake.connector import connect, DictCursor
from snowflake.connector.errors import ProgrammingError

from kendo.schemas.common import ICaughtException

snowflake.connector.paramstyle = "qmark"

from kendo.backends.connection import IBackendConnection


class SnowflakeBackendConnection(IBackendConnection):
    session: Any = None
    connection_name: str

    def __init__(self, connection_name: str):
        self.connection_name = connection_name
        self.session = self.get_session()

    def get_session(self):
        return self.session or connect(connection_name=self.connection_name)

    def execute(
        self,
        sql,
        sql_params=None,
        print_sql=False,
        abort_on_exception=True,
    ):
        with self.session.cursor(DictCursor) as cur:
            try:
                if print_sql:
                    print("--------------------")
                    print(sql)
                    print("--------------------")
                res = cur.execute(sql, sql_params).fetchall()
                # print(json.dumps(res, indent=4, sort_keys=True, default=str))
                return res
            except ProgrammingError as e:
                if abort_on_exception:
                    print(e)
                    raise typer.Abort()
                else:
                    return ICaughtException(message=str(e))

    def execute_many_times(
        self,
        sql,
        list_of_sql_params=None,
        print_sql=False,
        abort_on_exception=True,
    ):
        with self.session.cursor(DictCursor) as cur:
            try:
                if print_sql:
                    print("--------------------")
                    print(sql)
                    print("--------------------")
                res = cur.executemany(sql, list_of_sql_params).fetchall()
                # print(json.dumps(res, indent=4, sort_keys=True, default=str))
                return res
            except ProgrammingError as e:
                if abort_on_exception:
                    print(e)
                    raise typer.Abort()
                else:
                    return ICaughtException(message=str(e))

    def execute_multi_stmts(
        self,
        sql,
        print_sql=False,
        abort_on_exception=True,
    ):
        try:
            if print_sql:
                print("--------------------")
                print(sql)
                print("--------------------")
            cursors = self.session.execute_string(sql)
            return cursors
        except ProgrammingError as e:
            if abort_on_exception:
                print(e)
                raise typer.Abort()
            else:
                return ICaughtException(message=str(e))

    def close_session(self):
        self.session.close()
