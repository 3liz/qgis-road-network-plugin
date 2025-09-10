__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"

import os

from qgis.core import (
    QgsExpressionContextUtils,
    QgsProcessingException,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterProviderConnection,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)

from roadnetwork.plugin_tools import (
    available_migrations,
    plugin_path,
    plugin_test_data_path,
)
from roadnetwork.processing.algorithms.database.base import BaseDatabaseAlgorithm
from roadnetwork.qgis_plugin_tools.tools.i18n import tr
from roadnetwork.qgis_plugin_tools.tools.version import version

SCHEMA = "road_graph"


class CreateDatabaseStructure(BaseDatabaseAlgorithm):
    """
    Create the plugin structure in Database
    """

    CONNECTION_NAME = 'CONNECTION_NAME'
    OVERRIDE = 'OVERRIDE'
    ADD_TEST_DATA = 'ADD_TEST_DATA'

    OUTPUT_STATUS = 'OUTPUT_STATUS'
    OUTPUT_STRING = 'OUTPUT_STRING'

    def name(self):
        return 'create_database_structure'

    def displayName(self):
        return tr('Create database structure')

    def shortHelpString(self):
        short_help = tr(
            'Install the plugin database structure with tables and function on the chosen database.'
            '\n'
            '\n'
            'This script will add a schema with the needed tables and functions'
            '\n'
            '\n'
            '* PostgreSQL connection to the database: name of the database connection you would like to use for the installation.'
            '\n'
            '\n'
            'Beware ! If you check the "override" checkboxes, you will loose all existing data in the existing schema !'
        )
        return short_help

    def initAlgorithm(self, config):

        project = QgsProject.instance()
        connection_name = QgsExpressionContextUtils.projectScope(project).variable('roadnetwork_connection_name')
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("Connection to the PostgreSQL database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(tr(f"The database where the schema '{SCHEMA}' will be installed."))
        self.addParameter(param)

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.OVERRIDE,
                tr('Overwrite the database schema and all data ? ** CAUTION ** It will remove all existing data !'),
                defaultValue=False,
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.ADD_TEST_DATA,
                tr('Add test data ?'),
                defaultValue=False,
            )
        )

        # OUTPUTS
        # Add output for status (integer) and message (string)
        self.addOutput(
            QgsProcessingOutputNumber(
                self.OUTPUT_STATUS,
                tr('Output status')
            )
        )
        self.addOutput(
            QgsProcessingOutputString(
                self.OUTPUT_STRING,
                tr('Output message')
            )
        )

    def checkParameterValues(self, parameters, context):
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        connection = metadata.findConnection(connection_name)
        override = self.parameterAsBoolean(parameters, self.OVERRIDE, context)

        if SCHEMA in connection.schemas() and not override:
            msg = tr(f"Schema {SCHEMA} already exists in database ! If you REALLY want to drop and recreate it (and loose all data), check the *Overwrite* checkbox")
            return False, msg

        return super(CreateDatabaseStructure, self).checkParameterValues(parameters, context)

    def processAlgorithm(self, parameters, context, feedback):

        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )

        # noinspection PyTypeChecker
        connection = metadata.findConnection(connection_name)
        if not connection:
            raise QgsProcessingException(
                f"La connexion {connection_name} n'existe pas."
            )

        # Drop schema if needed
        override = self.parameterAsBool(parameters, self.OVERRIDE, context)
        if override:
            feedback.pushInfo(tr(f"Trying to drop schema {SCHEMA}…"))
            sql = f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;"
            metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
            connection_name = self.parameterAsConnectionName(
                parameters, self.CONNECTION_NAME, context
            )
            try:
                connection.executeSql(sql)
            except QgsProviderConnectionException as e:
                raise QgsProcessingException(str(e))
            feedback.pushInfo("  Success !")

        # Create full structure
        sql_files = [
            "00_initialize_database.sql",
            f"{SCHEMA}/10_FUNCTION.sql",
            f"{SCHEMA}/20_TABLE_SEQUENCE_DEFAULT.sql",
            f"{SCHEMA}/30_VIEW.sql",
            f"{SCHEMA}/40_INDEX.sql",
            f"{SCHEMA}/50_TRIGGER.sql",
            f"{SCHEMA}/60_CONSTRAINT.sql",
            f"{SCHEMA}/70_COMMENT.sql",
            f"{SCHEMA}/90_GLOSSARY.sql",
            "99_finalize_database.sql",
        ]
        # Add test data
        add_test_data = self.parameterAsBool(parameters, self.ADD_TEST_DATA, context)
        if add_test_data:
            sql_files.append("99_test_data.sql")

        plugin_dir = plugin_path()
        plugin_version = version()
        dev_version = False
        # Environment variable which can override
        # the source directory of SQL files to run
        overridden_plugin_version = os.environ.get(
            f"{SCHEMA.upper()}_OVERRIDDEN_PLUGIN_VERSION"
        )
        if plugin_version in ["master", "dev"] and not overridden_plugin_version:
            feedback.reportError(
                "Be careful, running the install on a development branch!"
            )
            dev_version = True

        # If we install SQL files from the first version
        # we should use the files inside plugin_test_data_path()
        if overridden_plugin_version:
            plugin_dir = plugin_test_data_path()
            feedback.reportError(
                f"Be careful, running migrations on an empty database using version {overridden_plugin_version} "
                f"instead of {plugin_version}"
            )
            plugin_version = overridden_plugin_version

        # Loop sql files and run SQL code
        for sf in sql_files:
            feedback.pushInfo(sf)
            sql_file = os.path.join(plugin_dir, f"install/sql/{sf}")
            with open(sql_file, "r") as f:
                sql = f.read()
                if len(sql.strip()) == 0:
                    feedback.pushInfo("  Skipped (empty file)")
                    continue

                try:
                    connection.executeSql(sql)
                except QgsProviderConnectionException as e:
                    raise QgsProcessingException(str(e))

                feedback.pushInfo("  Success !")

        # Add version
        if overridden_plugin_version or not dev_version:
            metadata_version = plugin_version
        else:
            migrations = available_migrations(000000)
            if migrations:
                last_migration = migrations[-1]
                metadata_version = (
                    last_migration.replace("upgrade_to_", "").replace(".sql", "").strip()
                )
                feedback.reportError(f"Latest migration is {metadata_version}")
            else:
                metadata_version = plugin_version

        sql = f"""
            INSERT INTO {SCHEMA}.metadata
            (id, me_version, me_version_date, me_status)
            VALUES (
                1, '{metadata_version}', now()::timestamp(0), 1
            )"""

        try:
            connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        feedback.pushInfo(
            f"Database version '{metadata_version}'."
        )

        return {
            self.OUTPUT_STATUS: 1,
            self.OUTPUT_STRING: tr(
                f"*** THE STRUCTURE {SCHEMA} HAS BEEN CREATED WITH VERSION '{metadata_version}'***"
            ),
        }
