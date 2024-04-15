from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

from .enums import TagableType


class ITagableObj(BaseModel):
    type: TagableType
    path: str = Field(min_length=1)


class ITagAssignmentRequest(BaseModel):
    tag: str = Field(min_length=1)
    value: str = Field(min_length=1)
    objects: List[ITagableObj]

    @field_validator("objects")
    @classmethod
    def objects_length(cls, v):
        if len(v) == 0:
            raise ValueError("objects cannot be empty")
        return v
