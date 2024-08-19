"""tableau tap class."""

from __future__ import annotations

import re

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import TapCapabilities

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition

from tap_tableau.client import HyperStream

class Taptableau(Tap):
    """tableau tap class."""

    name = "tap-tableau"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "hyper_file_path",
            th.StringType,
            required=True,
            description="The file path to the latest hyper file downloaded from Tableau",
        )
    ).to_dict()

    @classproperty
    def capabilities(self) -> list[TapCapabilities]:
        """Get tap capabilities"""
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.DISCOVER,
        ]
    
    def get_table_definitions(self) -> list[TableDefinition]:
        """Return a list of table defintions"""
        with HyperProcess(telemetry=False) as hyper:
            with Connection(hyper.endpoint, self.config.get("hyper_file_path")) as connection:
                return [connection.catalog.get_table_definition(table_name) for table_name in connection.catalog.get_table_names("Extract")]
            
    def get_table_entity(self, table_definition: TableDefinition) -> str:
        """Extracts table entity from hyper table definition"""
        name_split = str(table_definition.table_name).split('"."')
        entity = re.findall('^(.+)_[A-Z0-9]{32}"$', name_split[2])[0]
        return entity

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            HyperStream(
                tap=self,
                file_path=self.config.get("hyper_file_path"),
                table_definition=table_definition,
                name=self.get_table_entity(table_definition)
            )
            for table_definition in self.get_table_definitions()
        ]


if __name__ == "__main__":
    Taptableau.cli()
