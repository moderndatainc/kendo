from sqlalchemy import Column, Integer, Sequence, String
from kendo.snowflake.models.base import Base

database_objs_id_sequence = Sequence("database_objs_id_sequence")


class DatabaseObjModel(Base):  # type: ignore
    __tablename__ = "database_objs"
    # comment out the line specifying schema below for duckdb
    __table_args__ = {"schema": "infrastructure"}

    id = Column(
        Integer,
        database_objs_id_sequence,
        server_default=database_objs_id_sequence.next_value(),
        primary_key=True,
    )
    name = Column(String)

    def __repr__(self):
        return "<DatabaseObj(id='%s', name='%s')>" % (self.id, self.name)
