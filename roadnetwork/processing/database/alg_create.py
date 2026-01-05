
from pathlib import Path

from qgis.core import (
    QgsProcessingException,
    QgsProcessingFeedback,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterProviderConnection,
    QgsProcessingParameterString,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)

from ..tools import get_connection_name
from .base import BaseDatabaseAlgorithm, i18n, resources

# Shorcut
tr = i18n.tr


class CreateDatabaseStructure(BaseDatabaseAlgorithm):
    """
    Create the plugin structure in Database
    """

    CONNECTION_NAME = "CONNECTION_NAME"
    OVERRIDE = "OVERRIDE"
    SCHEMA = "SCHEMA"

    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"
    OUTPUT_VERSION = "OUTPUT_VERSION"

    def name(self):
        return "create_database_structure"

    def displayName(self):
        return tr("Create database structure")

    def shortHelpString(self):
        short_help = tr(
            "Install the plugin database structure with tables and function on "
            "the chosen database."
            "\n"
            "\n"
            "This script will add a schema with the needed tables and functions"
            "\n"
            "\n"
            "* PostgreSQL connection to the database: name of the database "
            "connection you would like to use for the installation."
            "\n"
            "\n"
            'Beware ! If you check the "override" checkboxes, you will loose '
            "all existing data in the existing schema !"
        )
        return short_help

    def initAlgorithm(self, config):
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("Connection to the PostgreSQL database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(tr("The database where the schema will be installed."))
        self.addParameter(param)

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.OVERRIDE,
                tr(
                    "Overwrite the database schema and all data ? "
                    "** CAUTION ** It will remove all existing data !"
                ),
                defaultValue=False,
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.SCHEMA,
                tr("Schema name"),
                defaultValue=resources.schema_name(),
            ),
        )

        # OUTPUTS
        # Add output for status (integer) and message (string)
        self.addOutput(QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status")))
        self.addOutput(QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message")))
        self.addOutput(QgsProcessingOutputNumber(self.OUTPUT_VERSION, tr("Output version")))

    def checkParameterValues(self, parameters, context):
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(
            parameters,
            self.CONNECTION_NAME,
            context,
        )
        connection = metadata.findConnection(connection_name)
        override = self.parameterAsBoolean(parameters, self.OVERRIDE, context)
        schema = self.parameterAsString(parameters, self.SCHEMA, context)

        if schema in connection.schemas() and not override:
            msg = tr(
                f"Schema {schema} already exists in database ! "
                "If you REALLY want to drop and recreate it (and loose all data), "
                "check the *Overwrite* checkbox"
            )
            return False, msg

        return super(CreateDatabaseStructure, self).checkParameterValues(parameters, context)

    @staticmethod
    def create_database(
        connection_name: str,
        schema: str,
        *,
        version: int,
        override: bool,
        install_dir: Path,
        feedback: QgsProcessingFeedback,
    ):
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            raise QgsProcessingException(f"La connexion {connection_name} n'existe pas.")

        # Drop schema if needed
        if override:
            feedback.pushInfo(tr(f"Trying to drop schema {schema}…"))
            sql = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
            try:
                connection.executeSql(sql)
            except QgsProviderConnectionException as e:
                raise QgsProcessingException(str(e))
            feedback.pushInfo("  Success !")

        # Create full structure
        # SCHEMA is used in the path even if the target schema is not the same
        plugin_schema_name = resources.schema_name()
        sql_files = [
            "00_initialize_database.sql",
            f"{plugin_schema_name}/10_FUNCTION.sql",
            f"{plugin_schema_name}/20_TABLE_SEQUENCE_DEFAULT.sql",
            f"{plugin_schema_name}/30_VIEW.sql",
            f"{plugin_schema_name}/40_INDEX.sql",
            f"{plugin_schema_name}/50_TRIGGER.sql",
            f"{plugin_schema_name}/60_CONSTRAINT.sql",
            f"{plugin_schema_name}/70_COMMENT.sql",
            f"{plugin_schema_name}/90_GLOSSARY.sql",
            "99_finalize_database.sql",
        ]

        # Loop sql files and run SQL code
        for sf in sql_files:
            feedback.pushInfo(sf)
            sql_file = install_dir.joinpath(sf)
            with open(sql_file, "r") as f:
                sql = f.read()
                if len(sql.strip()) == 0:
                    feedback.pushInfo("  Skipped (empty file)")
                    continue

                # Replace default SCHEMA by user defined one
                # Useful when the SQL calls functions or objects
                # prefixed by the schema
                if schema != plugin_schema_name:
                    sql = sql.replace(f"{plugin_schema_name}.", f"{schema}.")
                    sql = sql.replace(f" {plugin_schema_name};", f" {schema};")

                try:
                    connection.executeSql(sql)
                except QgsProviderConnectionException as e:
                    raise QgsProcessingException(str(e))

                feedback.pushInfo("  Success !")

        metadata_version = version
        sql = f"""
            INSERT INTO {schema}.metadata
            (id, me_version, me_version_date, me_status)
            VALUES (
                1, '{metadata_version}', now()::timestamp(0), 1
            )"""

        try:
            connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

    def processAlgorithm(self, parameters, context, feedback):
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        schema = self.parameterAsString(parameters, self.SCHEMA, context)
        override = self.parameterAsBool(parameters, self.OVERRIDE, context)
        install_dir = resources.plugin_path().joinpath("install", "sql")
        version = resources.schema_version()

        self.create_database(
            connection_name,
            schema,
            version=version,
            override=override,
            install_dir=install_dir,
            feedback=feedback,
        )

        feedback.pushInfo(f"Database version '{version}'.")

        # Launch also the algorithme on the editing_session schema
        if feedback.isCanceled():
            return {}

        self.create_database(
            connection_name,
            'editing_session',
            version=version,
            override=override,
            install_dir=install_dir,
            feedback=feedback,
        )

        return {
            self.OUTPUT_STATUS: 1,
            self.OUTPUT_VERSION: version,
            self.OUTPUT_STRING: tr(
                f"*** THE STRUCTURE {schema} HAS BEEN CREATED WITH VERSION '{version}'***"
            ),
        }
