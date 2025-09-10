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
    format_version_integer,
    plugin_path,
)
from roadnetwork.processing.algorithms.database.base import BaseDatabaseAlgorithm
from roadnetwork.qgis_plugin_tools.tools.i18n import tr
from roadnetwork.qgis_plugin_tools.tools.version import version

SCHEMA = "road_graph"


class UpgradeDatabaseStructure(BaseDatabaseAlgorithm):

    CONNECTION_NAME = 'CONNECTION_NAME'
    RUN_MIGRATIONS = 'RUN_MIGRATIONS'
    OUTPUT_STATUS = 'OUTPUT_STATUS'
    OUTPUT_STRING = 'OUTPUT_STRING'

    def name(self):
        return 'upgrade_database_structure'

    def displayName(self):
        return tr('Upgrade database structure')

    def shortHelpString(self):
        short_help = tr(
            'Upgrade the plugin tables and functions in the chosen database.'
            '\n'
            '\n'
            'If you have upgraded your QGIS plugin, you can run this script'
            ' to upgrade your database to the new plugin version.'
            '\n'
            '\n'
            '* PostgreSQL connection to the database: name of the database connection you would like to use for the upgrade.'
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
        param.setHelp(tr("The database where the schema '{}' will be installed.").format(SCHEMA))
        self.addParameter(param)

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.RUN_MIGRATIONS,
                tr('Check this box to upgrade. No action will be done otherwise'),
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
        # Check if runit is checked
        run_migrations = self.parameterAsBool(parameters, self.RUN_MIGRATIONS, context)
        if not run_migrations:
            msg = tr('You must check the box to run the upgrade !')
            return False, msg

        # Check that the connection name has been configured
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        if not connection_name:
            return False, tr('You must use the "Configure plugin" alg to set the database connection name')

        connection = metadata.findConnection(connection_name)

        # # Check that it corresponds to an existing connection
        # if connection_name not in get_postgis_connection_list():
        #     return False, tr('The configured connection name does not exists in QGIS')

        if SCHEMA in connection.schemas() and not run_migrations:
            msg = tr(f"Schema {SCHEMA} already exists in database ! If you REALLY want to drop and recreate it (and loose all data), check the *Overwrite* checkbox")
            return False, msg

        return super(UpgradeDatabaseStructure, self).checkParameterValues(parameters, context)

    def processAlgorithm(self, parameters, context, feedback):

        # Run migration
        run_migrations = self.parameterAsBool(parameters, self.RUN_MIGRATIONS, context)
        if not run_migrations:
            msg = tr("Vous devez cocher cette case pour réaliser la mise à jour !")
            raise QgsProcessingException(msg)

        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        connection = metadata.findConnection(connection_name)

        # Get database version
        sql = """
            SELECT me_version
            FROM {}.metadata
            WHERE me_status = 1
            ORDER BY me_version_date DESC
            LIMIT 1;
        """.format(
            SCHEMA
        )
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        db_version = None
        for a in data:
            db_version = a[0]
        if not db_version:
            error_message = tr("No installed version found in the database !")
            raise QgsProcessingException(error_message)

        feedback.pushInfo(
            tr("Database structure version") + " = {}".format(db_version)
        )

        # Get plugin version
        plugin_version = version()
        if plugin_version in ["master", "dev"]:
            migrations = available_migrations(000000)
            if migrations:
                last_migration = migrations[-1]
                plugin_version = (
                    last_migration.replace("upgrade_to_", "").replace(".sql", "").strip()
                )
                feedback.reportError(
                    "Be careful, running the migrations on a development branch!"
                )
                feedback.reportError(
                    "Latest available migration is {}".format(plugin_version)
                )

        else:
            feedback.pushInfo(tr("Plugin version") + " = {}".format(plugin_version))

        # Return if nothing to do
        if db_version == plugin_version:
            return {
                self.OUTPUT_STATUS: 1,
                self.OUTPUT_STRING: tr(
                    " The database version already matches the plugin version. No upgrade needed."
                ),
            }

        db_version_integer = format_version_integer(db_version)
        sql_files = available_migrations(db_version_integer)

        # Loop sql files and run SQL code
        for sf in sql_files:
            sql_file = os.path.join(plugin_path(), "install/sql/upgrade/{}".format(sf))
            with open(sql_file, "r") as f:
                sql = f.read()
                if len(sql.strip()) == 0:
                    feedback.pushInfo('* ' + sf + ' -- SKIPPED (EMPTY FILE)')
                    continue

                # Add SQL database version in adresse.metadata
                new_db_version = (
                    sf.replace("upgrade_to_", "").replace(".sql", "").strip()
                )
                feedback.pushInfo(tr("* NEW DB VERSION ") + new_db_version)
                sql += """
                    UPDATE {}.metadata
                    SET (me_version, me_version_date)
                    = ( '{}', now()::timestamp(0) );
                """.format(
                    SCHEMA, new_db_version
                )

                try:
                    connection.executeSql(sql)
                except QgsProviderConnectionException as e:
                    feedback.reportError("Error when executing file {}".format(sf))
                    connection.executeSql('ROLLBACK;')
                    raise QgsProcessingException(str(e))

                feedback.pushInfo("* " + sf + " -- OK !")

        # Everything is fine, we now update to the plugin version
        sql = """
            UPDATE {}.metadata
            SET (me_version, me_version_date)
            = ( '{}', now()::timestamp(0) );
        """.format(
            SCHEMA, plugin_version
        )

        try:
            connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        msg = tr("*** THE DATABASE STRUCTURE HAS BEEN UPDATED ***")
        feedback.pushInfo(msg)

        return {self.OUTPUT_STATUS: 1, self.OUTPUT_STRING: msg}
