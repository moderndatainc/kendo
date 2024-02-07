from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

from .enums import TagAssignmentObject


class ITagAssignmentObject(BaseModel):
    type: TagAssignmentObject
    path: str = Field(min_length=1)


class ITagAssignment(BaseModel):
    tag: str = Field(min_length=1)
    value: str = Field(min_length=1)
    objects: List[ITagAssignmentObject]

    @field_validator("objects")
    @classmethod
    def objects_length(cls, v):
        if len(v) == 0:
            raise ValueError("objects cannot be empty")
        return v
