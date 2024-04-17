from sqlalchemy import Column, Integer, Sequence, String
from kendo.snowflake.models.base import Base

tags_id_sequence = Sequence("tags_id_sequence")


class TagModel(Base):  # type: ignore
    __tablename__ = "tags"
    # comment out the line specifying schema below for duckdb
    __table_args__ = {"schema": "config"}

    id = Column(
        Integer,
        tags_id_sequence,
        server_default=tags_id_sequence.next_value(),
        primary_key=True,
    )
    name = Column(String)

    def __repr__(self):
        return "<Tag(id='%s', name='%s')>" % (self.id, self.name)
