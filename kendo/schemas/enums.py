from enum import Enum


class TagableType(Enum):
    USER = "user"
    ROLE = "role"
    TABLE = "table"
    COLUMN = "column"
    VIEW = "view"

class BackendProvider(str, Enum):
    snowflake = "snowflake"