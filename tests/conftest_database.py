"""Base class for tests using a database."""
import os

from pathlib import Path
from typing import Optional, Sequence

import psycopg
import pytest

from qgis.core import QgsApplication

from roadnetwork.plugin_tools import resources
from roadnetwork.processing.provider import Provider


# Return the latest upgrade version
@pytest.fixture(scope="session")
def db_install_version() -> Optional[int]:
    version = os.getenv("DB_INSTALL_VERSION")
    if version is not None:
        return int(version)
    else:
        latest = resources.latest_upgrade()
        return latest[0] if latest else None


# Return the schema defined in environment
@pytest.fixture(scope="session")
def db_schema() -> str:
    return os.getenv("SCHEMA", resources.schema_name())


# Register the processing provider once
@pytest.fixture(scope="session")
def processing_provider() -> Provider:
    """Initialize processing"""
    provider = Provider()

    registry = QgsApplication.processingRegistry()
    registry.addProvider(provider)

    provider_id = provider.id()

    assert registry.algorithmById(f"{provider_id}:create_database_structure") is not None
    assert registry.algorithmById(f"{provider_id}:upgrade_database_structure") is not None

    return provider


@pytest.fixture(scope="session")
def db_test_sql(data: Path) -> Sequence[Path]:
    """Return the list of sql scripts to run
    when initializing database for tests
    """
    # return (data.joinpath("install","sql","99_test_data.sql"),)
    return ()


# The following is executed  in each test
#
# Initialize (Override existing) and return a db
# connection
@pytest.fixture()
def db_connection() -> psycopg.Connection:
    """Initialize (Override existing) and return a db connection"""
    connection = psycopg.connect(
        user="docker",
        password="docker",
        host="db",
        port="5432",
        dbname="gis"
    )

    return connection
