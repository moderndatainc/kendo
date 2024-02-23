from typing import List, Optional
from pydantic import BaseModel, Field


class ISelect(BaseModel):
    table: str = Field(min_length=1)
    select: Optional[List[str]] = None
    where: Optional[str] = None


class IInsert(BaseModel):
    table: str = Field(min_length=1)
    data: dict


class IInsertV2(BaseModel):
    table: str = Field(min_length=1)
    columns: List[str]


class IInsertBulk(BaseModel):
    table: str = Field(min_length=1)
    data: List[dict]


def _quote_if_str(val):
    if type(val) is str:
        return f"'{val}'"
    else:
        # val is not a string, but we need to stringify it to construct the SQL anyway
        return str(val)


# TODO: remove invocations where user input is used
def generate_select(input: ISelect, include_semicolon: bool = True) -> str:
    sql = (
        f"SELECT {', '.join(input.select) if input.select else '*'} FROM {input.table}"
    )
    if input.where:
        sql += f" WHERE {input.where}"
    if include_semicolon:
        sql += ";"
    return sql


# TODO: remove invocations where user input is used
def generate_insert(input: IInsert, include_semicolon: bool = True) -> str:
    values = list(map(_quote_if_str, input.data.values()))
    sql = f"INSERT INTO {input.table} ({', '.join(input.data.keys())}) VALUES ({', '.join(values)})"
    if include_semicolon:
        sql += ";"
    return sql


# Use this where input from user is involved
def generate_insert_v2(input: IInsertV2) -> str:
    qmark_sql = f"INSERT INTO {input.table} ({', '.join(input.columns)}) VALUES ({', '.join(['?' for _ in input.columns])})"
    return qmark_sql


# TODO: remove invocations where user input is used
def generate_insert_bulk(input: IInsertBulk, include_semicolon: bool = True) -> str:
    values = [list(map(_quote_if_str, row.values())) for row in input.data]
    sql = f"INSERT INTO {input.table} ({', '.join(input.data[0].keys())}) VALUES"
    for i, row in enumerate(values):
        sql += f" ({', '.join(row)})"
        if i < len(values) - 1:
            sql += ","
    if include_semicolon:
        sql += ";"
    return sql
