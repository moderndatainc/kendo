from datetime import datetime
from typing import NotRequired, TypedDict


class DatabaseObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str


class SchemaObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    DATABASE_ID: int
    DATABASE_NAME: NotRequired[str]


class TableObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    SCHEMA_ID: int
    SCHEMA_NAME: NotRequired[str]
    DATABASE_ID: NotRequired[int]
    DATABASE_NAME: NotRequired[str]
    KIND: NotRequired[str]
    COMMENT: NotRequired[str]
    CLUSTER_BY: NotRequired[str]
    NUM_ROWS: NotRequired[int]
    NUM_BYTES: NotRequired[int]
    OWNER: NotRequired[str]
    OWNER_ROLE_ID: NotRequired[int]
    RETENTION_TIME: NotRequired[str]
    AUTOMATIC_CLUSTERING: NotRequired[str]
    CHANGE_TRACKING: NotRequired[str]
    IS_EXTERNAL: NotRequired[str]
    ENABLE_SCHEMA_EVOLUTION: NotRequired[str]
    OWNER_ROLE_TYPE: NotRequired[str]
    IS_EVENT: NotRequired[str]
    BUDGET: NotRequired[str]
    IS_HYBRID: NotRequired[str]
    IS_ICEBERG: NotRequired[str]
    IS_DYNAMIC: NotRequired[str]
    DDL: NotRequired[str]


class StageObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    SCHEMA_ID: int
    SCHEMA_NAME: NotRequired[str]
    DATABASE_ID: NotRequired[int]
    DATABASE_NAME: NotRequired[str]


class StreamObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    TABLE_NAME: NotRequired[str]
    SCHEMA_ID: int
    SCHEMA_NAME: NotRequired[str]
    DATABASE_ID: NotRequired[int]
    DATABASE_NAME: NotRequired[str]


class PipeObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    SCHEMA_ID: int
    SCHEMA_NAME: NotRequired[str]
    DATABASE_ID: NotRequired[int]
    DATABASE_NAME: NotRequired[str]


class ColumnObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    TABLE_ID: int
    TABLE_NAME: NotRequired[str]
    SCHEMA_ID: NotRequired[int]
    SCHEMA_NAME: NotRequired[str]
    DATABASE_ID: NotRequired[int]
    DATABASE_NAME: NotRequired[str]


class RoleObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str


class UserObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    LAST_SUCCESS_LOGIN: NotRequired[datetime]
    LOGIN_NAME: str
    OWNER_ROLE_ID: NotRequired[int]
    OWNER_ROLE_NAME: NotRequired[str]
    EMAIL: NotRequired[str]
    DEFAULT_ROLE_ID: NotRequired[int]
    DEFAULT_ROLE_NAME: NotRequired[str]
    EXT_AUTHN_UID: NotRequired[str]
    IS_EXT_AUTHN_DUO: NotRequired[bool]


class PrivilegeGrantObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    PRIVILEGE: str
    GRANTED_ON: str
    GRANTED_ON_ID: int
    GRANTED_ON_NAME: NotRequired[str]
    GRANTED_TO: str
    GRANTED_TO_ID: int
    GRANTED_TO_NAME: NotRequired[str]
    GRANT_OPTION: bool


class RoleGrantObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    ROLE_ID: int
    ROLE: NotRequired[str]
    GRANTED_TO: str
    GRANTED_TO_ID: int
    GRANTEE_NAME: NotRequired[str]
    GRANTED_BY_ROLE_ID: NotRequired[int]
    GRANTED_BY: NotRequired[str]


class WarehouseObj(TypedDict):
    ID: int
    OBJ_CREATED_ON: NotRequired[datetime]
    NAME: str
    TYPE: str
    SIZE: str
    OWNER_ROLE_ID: NotRequired[int]
    OWNER_ROLE_NAME: NotRequired[str]