from enum import Enum


class TagableType(Enum):
    USER = "user"
    ROLE = "role"
    TABLE = "table"
    COLUMN = "column"
    VIEW = "view"

class BackendDB(str, Enum):
    snowflake = "snowflake"
    duckdb = "duckdb"
