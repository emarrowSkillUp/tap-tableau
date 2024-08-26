"""Custom client handling, including tableauStream base class."""

from __future__ import annotations

from os import PathLike
from typing import TYPE_CHECKING, Any, Iterable
from datetime import timezone

from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Name

from singer_sdk import Tap
from singer_sdk._singerlib.schema import Schema
from singer_sdk.streams import Stream
from singer_sdk import typing as th

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

hyper_singer_mapping = {
    SqlType.bool(): th.BooleanType,
    SqlType.text(): th.StringType,
    SqlType.timestamp(): th.DateTimeType,
    SqlType.big_int(): th.IntegerType,
    SqlType.double(): th.NumberType,
    SqlType.float(): th.NumberType,
    SqlType.int(): th.IntegerType
}


class HyperStream(Stream):
    """Stream class for tableau streams."""
    primary_keys = ["_id"]
    is_sorted = True

    def __init__(self, file_path: str, table_definition: TableDefinition, *args, **kwargs):
        """Init HyperStream"""
        self.file_path = file_path
        self.table_definition = table_definition
        super().__init__(*args, **kwargs)

    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """
        #header = [column.name.unescaped for column in self.table_definition.columns]
        for row in self.get_rows(context):
            yield {col.name.unescaped: (value if col.type != SqlType.timestamp() else value.astimezone(timezone.utc).to_datetime().isoformat()) for col, value in zip(self.table_definition.columns, row)}
    
    def get_rows(self, context: Context | None) -> Iterable[list]:
        """Return a generator of the rows in the Hyper table"""
        with HyperProcess(telemetry=False, parameters={'log_config': ''}) as hyper:
            with Connection(hyper.endpoint, self.file_path) as connection:
                if bookmark := self.get_starting_timestamp(context):
                    with connection.execute_query(f'select * from {self.table_definition.table_name} where {Name(self.replication_key)} >= \'{bookmark.isoformat(" ")}\' order by {Name(self.replication_key)}') as result:
                        yield from result
                else:
                    with connection.execute_query(f'select * from {self.table_definition.table_name} order by {Name(self.replication_key)}') as result:
                        yield from result

    @property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved
        """
        properties: list[th.Property] = []

        properties.extend(th.Property(column.name.unescaped, hyper_singer_mapping.get(column.type)) for column in self.table_definition.columns)
        return th.PropertiesList(*properties).to_dict()