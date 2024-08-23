"""tableau tap class."""

from __future__ import annotations

import re
from pathlib import Path
import os

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
            "hyper_dir",
            th.StringType,
            required=True,
            description="Directory containing hyper files"
        )
    ).to_dict()

    @classproperty
    def capabilities(self) -> list[TapCapabilities]:
        """Get tap capabilities"""
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.DISCOVER,
        ]
    
    def get_hyper_files(self) -> list[str]:
        """Returns a list of paths to the downloaded hyperfiles"""
        hyper_dir = Path(self.config.get('hyper_dir'))
        return [os.path.join(self.config.get('hyper_dir'), f.name) for f in hyper_dir.iterdir() if f.is_file()]
    
    def get_table_definitions(self, hyper_file) -> list[TableDefinition]:
        """Returns a list of table defintions for a given hyper file"""
        with HyperProcess(telemetry=False, parameters={'log_config': ''}) as hyper:
            with Connection(hyper.endpoint, hyper_file) as connection:
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
                file_path=hyper_file,
                table_definition=table_definition,
                name=self.get_table_entity(table_definition)
            )
            for hyper_file in self.get_hyper_files()
            for table_definition in self.get_table_definitions(hyper_file)
        ]


if __name__ == "__main__":
    Taptableau.cli()
