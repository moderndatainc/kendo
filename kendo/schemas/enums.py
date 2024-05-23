from enum import Enum


class TagableType(Enum):
    USER = "user"
    ROLE = "role"
    TABLE = "table"
    COLUMN = "column"
    VIEW = "view"


class BackendProvider(str, Enum):
    snowflake = "snowflake"


class Resources(str, Enum):
    databases = "databases"
    schemas = "schemas"
    tables = "tables"
    columns = "columns"
    users = "users"
    roles = "roles"
    grants_to_roles = "grants_to_roles"
    role_grants = "role_grants"
    warehouses = "warehouses"
    stages = "stages"
    streams = "streams"
    pipes = "pipes"
    views = "views"
    all = "all"
