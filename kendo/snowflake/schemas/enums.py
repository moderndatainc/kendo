from enum import Enum


class TagableType(Enum):
    USER = "user"
    ROLE = "role"
    TABLE = "table"
    COLUMN = "column"
    VIEW = "view"
