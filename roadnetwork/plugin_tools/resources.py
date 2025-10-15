"""Tools to work with resources files."""

import configparser
import functools

from importlib import resources
from pathlib import Path
from typing import (
    Iterator,
    Optional,
    Sequence,
    Type,
    cast,
)

PACKAGE_NAME = "roadnetwork"


def plugin_path(*args: str | Path) -> Path:
    """Return the path to the plugin root folder."""
    # Use the canonical way to get module resources path
    return cast(Path, resources.files(PACKAGE_NAME)).joinpath(*args)


def resources_path(*args) -> Path:
    """Return the path to the plugin resources folder."""
    return plugin_path("resources", *args)


@functools.cache
def available_migrations(minimum_version: int = 0) -> Sequence[tuple[int, Path]]:
    """Get all the upgrade SQL files since the provided version."""
    upgrade_dir = plugin_path("install", "sql", "upgrade")

    def files() -> Iterator[tuple[int, Path]]:
        for sql_file in upgrade_dir.glob("*.sql"):
            if sql_file.name.startswith("upgrade_to_"):
                version = int(sql_file.stem.removeprefix("upgrade_to_"))
                if version > minimum_version:
                    yield (version, sql_file)

    return tuple(s for s in sorted(files(), key=lambda item: item[0]))  # type: ignore [call-overload]


def latest_upgrade() -> Optional[tuple[int, Path]]:
    """Get the latest install version from which to upgrade"""
    versions = available_migrations()
    return versions[-1] if versions else None


@functools.cache
def metadata_config() -> configparser.ConfigParser:
    """Parse the metatada file"""
    path = plugin_path("metadata.txt")
    config = configparser.ConfigParser()
    config.read(path)
    return config


def version(remove_prefix: bool = True) -> str:
    """Returns version defined in metatada.txt"""
    v = metadata_config()["general"]["version"]
    if remove_prefix:
        v = v.removeprefix("v")
    return v


def schema_version() -> int:
    return metadata_config().getint(plugin_name(), "schemaVersion", fallback=0)


def schema_name() -> str:
    return metadata_config().get(plugin_name(), "schemaName")


def plugin_name() -> str:
    return metadata_config()["general"]["name"]


normalized_chars = str.maketrans({" ": "_", "-": "_"})


def plugin_name_normalized() -> str:
    return plugin_name().lower().translate(normalized_chars)


#
# UI
#
def load_ui(*args: str) -> Type:
    """Get compiled ui file"""
    from qgis.PyQt import uic

    ui_class, _ = uic.loadUiType(resources_path("ui", *args))
    return ui_class
