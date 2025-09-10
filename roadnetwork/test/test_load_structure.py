"""Tests for Processing algorithms."""

import os
import time

import psycopg2

from qgis.core import Qgis, QgsApplication, QgsProcessingException
from qgis.testing import unittest

if Qgis.QGIS_VERSION_INT >= 30800:
    from qgis import processing
else:
    import processing

from roadnetwork.plugin_tools import available_migrations
from roadnetwork.processing.provider import RoadNetworkProvider as ProcessingProvider
from roadnetwork.qgis_plugin_tools.tools.logger_processing import (
    LoggerProcessingFeedBack,
)

__copyright__ = "Copyright 2020, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"
__revision__ = "$Format:%H$"

SCHEMA = "road_graph"

# Declare first version of the database structure
FIRST_VERSION = "0.1.0"

# This list must not be changed
# as it correspond to the list of tables
# created for the first version
TABLES_FOR_FIRST_VERSION = [
    "edges",
    "glossary_road_class",
    "markers",
    "metadata",
    "nodes",
    "roads",
    "v_road_without_zero_marker",
]

# Expected list of tables for current version
# Must be changed any time the SQL structure is changed
TABLES_FOR_CURRENT_VERSION = [
    "edges",
    "glossary_road_class",
    "markers",
    "metadata",
    "nodes",
    "roads",
    "v_road_without_zero_marker",
]


class TestProcessing(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = psycopg2.connect(
            user="docker", password="docker", host="db", port="5432", database="gis"
        )
        self.cursor = self.connection.cursor()

    def tearDown(self) -> None:
        del self.cursor
        del self.connection
        time.sleep(1)

    def test_create_database_structure_and_upgrade(self):
        """Test the algorithms for creating and updating the database structure."""

        # Add processing provider
        registry = QgsApplication.processingRegistry()
        provider = ProcessingProvider()
        if not registry.providerById(provider.id()):
            registry.addProvider(provider)

        # Check roadnetwork provider is loaded
        provider_names = [a.id() for a in registry.providers()]
        self.assertEqual(
            ('roadnetwork' in provider_names),
            True
        )

        # Run create database structure alg
        # We use the environment variable XXXXX_OVERRIDDEN_PLUGIN_VERSION
        # to install the SQL files corresponding to the first version
        feedback = LoggerProcessingFeedBack()
        params = {
            'CONNECTION_NAME': 'test',
            'OVERRIDE': True,
            'ADD_TEST_DATA': True,
        }
        os.environ[f"{SCHEMA.upper()}_OVERRIDDEN_PLUGIN_VERSION"] = FIRST_VERSION
        alg = f"{provider.id()}:create_database_structure"
        processing_output = processing.run(alg, params, feedback=feedback)
        del os.environ[f"{SCHEMA.upper()}_OVERRIDDEN_PLUGIN_VERSION"]

        # Check the list of tables and views from the database
        self.cursor.execute(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name
            """
        )
        records = self.cursor.fetchall()
        result = [r[0] for r in records]

        # Expected tables in the specific version written above at the beginning of the test.
        # DO NOT CHANGE HERE, change below at the end of the test.
        self.assertCountEqual(TABLES_FOR_FIRST_VERSION, result)
        self.assertEqual(TABLES_FOR_FIRST_VERSION, result)
        expected = f"*** THE STRUCTURE {SCHEMA} HAS BEEN CREATED WITH VERSION '{FIRST_VERSION}'***"
        self.assertEqual(expected, processing_output["OUTPUT_STRING"])

        # Check if the version has been written in the metadata table
        sql = f"""
            SELECT me_version
            FROM {SCHEMA}.metadata
            WHERE me_status = 1
            ORDER BY me_version_date DESC
            LIMIT 1;
        """
        self.cursor.execute(sql)
        record = self.cursor.fetchone()
        self.assertEqual(FIRST_VERSION, record[0])

        # Run the update database structure alg
        # Since the structure has been created with FIRST_VERSION above
        # The expected list of tables
        feedback.pushDebugInfo("Update the database")
        params = {
            "CONNECTION_NAME": "test",
            "RUN_MIGRATIONS": True
        }
        alg = f"{provider.id()}:upgrade_database_structure"
        results = processing.run(alg, params, feedback=feedback)
        self.assertEqual(1, results["OUTPUT_STATUS"], 1)
        self.assertEqual(
            "*** THE DATABASE STRUCTURE HAS BEEN UPDATED ***",
            results["OUTPUT_STRING"],
        )

        # Check the version has been updated
        sql = f"""
            SELECT me_version
            FROM {SCHEMA}.metadata
            WHERE me_status = 1
            ORDER BY me_version_date DESC
            LIMIT 1;
        """
        self.cursor.execute(sql)
        record = self.cursor.fetchone()

        migrations = available_migrations(000000)
        if migrations:
            last_migration = migrations[-1]
            metadata_version = (
                last_migration.replace("upgrade_to_", "").replace(".sql", "").strip()
            )
            self.assertEqual(metadata_version, record[0])

        # Check the list of tables
        self.cursor.execute(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name
            """
        )
        records = self.cursor.fetchall()
        result = [r[0] for r in records]
        self.assertCountEqual(TABLES_FOR_CURRENT_VERSION, result)

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
        alg = f"{provider.id()}:upgrade_database_structure"
        results = processing.run(alg, params, feedback=feedback)
        self.assertEqual(1, results["OUTPUT_STATUS"], 1)
        self.assertEqual(
            " The database version already matches the plugin version. No upgrade needed.",
            results["OUTPUT_STRING"],
        )

        # Check the list of tables
        self.cursor.execute(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name
            """
        )
        records = self.cursor.fetchall()
        result = [r[0] for r in records]
        self.assertCountEqual(TABLES_FOR_CURRENT_VERSION, result)
        self.assertEqual(TABLES_FOR_CURRENT_VERSION, result)
