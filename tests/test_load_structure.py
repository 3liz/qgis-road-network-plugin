"""Tests for Processing algorithms."""

import unittest

from pathlib import Path

import psycopg

from qgis import processing

from roadnetwork.plugin_tools.feedback import LoggerProcessingFeedBack
from roadnetwork.plugin_tools.resources import (
    available_migrations,
    schema_version,
)
from roadnetwork.processing.database import CreateDatabaseStructure
from roadnetwork.processing.provider import Provider

# This list must not be changed
# as it correspond to the list of tables
# created for the first version
TABLES_FOR_FIRST_VERSION = [
    "glossary_test_category",
    "metadata",
    "test",
]

# Expected list of tables for current version
# Must be changed any time the SQL structure is changed
TABLES_FOR_CURRENT_VERSION = [
    "glossary_test_category",
    "metadata",
    "test",
]


def test_processing_create(processing_provider: Provider):
    params = {
        "CONNECTION_NAME": "test",
        "OVERRIDE": True,
    }

    feedback = LoggerProcessingFeedBack()

    # Run create database structure alg
    alg = f"{processing_provider.id()}:create_database_structure"
    processing_output = processing.run(alg, params, feedback=feedback)

    assert processing_output["OUTPUT_STATUS"] == 1
    assert processing_output["OUTPUT_VERSION"] == schema_version()


def test_upgrade_from(
    db_schema: str,
    db_install_version: int,
    db_connection: psycopg.Connection,
    processing_provider: Provider,
    data: Path,
):
    """Test the algorithms for creating and updating the database structure."""

    current_version = schema_version()

    assert db_install_version is not None, "This test require at least one availabl upgrade"
    assert current_version >= db_install_version, (
        "Current schema version cannot be lower than install version"
    )

    # Get the installation dir
    install_dir = data.joinpath(f"install-version-{current_version}", "sql")
    assert install_dir.exists()

    feedback = LoggerProcessingFeedBack()

    # Create the database from the latest update
    CreateDatabaseStructure.create_database(
        "test",
        db_schema,
        version=db_install_version,
        override=True,
        install_dir=install_dir,
        feedback=feedback,
    )

    case = unittest.TestCase()

    provider_id = processing_provider.id()

    cursor = db_connection.cursor()

    # Check the list of tables and views from the database
    cursor.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_schema}'
        ORDER BY table_name
        """
    )
    records = cursor.fetchall()
    result = [r[0] for r in records]

    # Expected tables in the specific version written above at the beginning of the test.
    # DO NOT CHANGE HERE, change below at the end of the test.
    case.assertCountEqual(TABLES_FOR_FIRST_VERSION, result)

    assert result == TABLES_FOR_CURRENT_VERSION

    # Check if the version has been written in the metadata table
    sql = f"""
        SELECT me_version
        FROM {db_schema}.metadata
        WHERE me_status = 1
        ORDER BY me_version_date DESC
        LIMIT 1;
    """
    cursor.execute(sql)
    record = cursor.fetchone()
    assert record is not None
    assert int(record[0]) == db_install_version

    # Run the update database structure alg
    # Since the structure has been created with db_install_version above
    # The expected list of tables
    feedback.pushDebugInfo("Update the database")
    params = {
        "CONNECTION_NAME": "test",
        "RUN_MIGRATIONS": True
    }
    alg = f"{provider_id}:upgrade_database_structure"
    results = processing.run(alg, params, feedback=feedback)

    assert results["OUTPUT_STATUS"] == 1
    assert results["OUTPUT_STRING"] == "*** THE DATABASE STRUCTURE HAS BEEN UPDATED ***"

    # Check the version has been updated
    sql = f"""
        SELECT me_version
        FROM {db_schema}.metadata
        WHERE me_status = 1
        ORDER BY me_version_date DESC
        LIMIT 1;
    """
    cursor.execute(sql)
    record = cursor.fetchone()

    migrations = available_migrations()
    if migrations:
        version, _ = migrations[-1]
        assert record is not None
        assert int(record[0]) == version

    # Check the list of tables
    cursor.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_schema}'
        ORDER BY table_name
        """
    )
    records = cursor.fetchall()
    result = [r[0] for r in records]
    case.assertCountEqual(TABLES_FOR_CURRENT_VERSION, result)

    # Create the database structure with override
    # This will delete and recreate the structure for the last version
    feedback.pushDebugInfo("Relaunch the algorithm without override")
    params = {
        'CONNECTION_NAME': 'test',
        "OVERRIDE": True,
    }

    # Check we need to run upgrade or not
    feedback.pushDebugInfo("Update the database")
    params = {
        "CONNECTION_NAME": "test",
        "RUN_MIGRATIONS": True
    }
    alg = f"{provider_id}:upgrade_database_structure"
    results = processing.run(alg, params, feedback=feedback)
    assert results["OUTPUT_STATUS"] == 1
    assert results["OUTPUT_STRING"] == (
        " The database version already matches the plugin version. No upgrade needed."
    )

    # Check the list of tables
    cursor.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_schema}'
        ORDER BY table_name
        """
    )
    records = cursor.fetchall()
    result = [r[0] for r in records]

    case.assertCountEqual(TABLES_FOR_CURRENT_VERSION, result)

    assert result == TABLES_FOR_CURRENT_VERSION
