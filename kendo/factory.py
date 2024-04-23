from typing import Type
from kendo.backends.connection import IBackendConnection
from kendo.backends.crud import IParameterizedInsert, ISelect
from kendo.schemas.enums import BackendProvider


class Factory:
    backend_connection: IBackendConnection
    backend_DDL: str
    select: Type[ISelect]
    paramized_insert: Type[IParameterizedInsert]

    def __init__(self, config_doc: dict):
        if config_doc["backend"]["provider"] == BackendProvider.snowflake:
            from kendo.backends.snowflake.connection import SnowflakeBackendConnection
            from kendo.backends.snowflake.ddl import SQL

            self.backend_connection = SnowflakeBackendConnection(
                config_doc["datasource"]["connection_name"]
            )
            self.backend_DDL = SQL
        self.select = ISelect
        self.paramized_insert = IParameterizedInsert
