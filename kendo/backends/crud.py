from typing import List, Optional
from pydantic import BaseModel, Field


class ISelect(BaseModel):
    table: str = Field(min_length=1)
    columns: Optional[List[str]] = None
    where: Optional[str] = None

    def generate_statement(self, include_semicolon: bool = True) -> str:
        sql = (
            f"SELECT {', '.join(self.columns) if self.columns else '*'} FROM {self.table}"
        )
        if self.where:
            sql += f" WHERE {self.where}"
        if include_semicolon:
            sql += ";"
        return sql


class IParameterizedInsert(BaseModel):
    table: str = Field(min_length=1)
    columns: List[str]

    def generate_statement(self) -> str:
        parameterized_sql = f"INSERT INTO {self.table} ({', '.join(self.columns)}) VALUES ({', '.join(['?' for _ in self.columns])})"
        return parameterized_sql
