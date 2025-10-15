
from qgis.core import (
    QgsProcessingException,
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


class UpgradeDatabaseStructure(BaseDatabaseAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    RUN_MIGRATIONS = "RUN_MIGRATIONS"
    SCHEMA = "SCHEMA"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"

    def name(self):
        return "upgrade_database_structure"

    def displayName(self):
        return tr("Upgrade database structure")

    def shortHelpString(self):
        short_help = tr(
            "Upgrade the plugin tables and functions in the chosen database."
            "\n"
            "\n"
            "If you have upgraded your QGIS plugin, you can run this script"
            " to upgrade your database to the new plugin version."
            "\n"
            "\n"
            "* PostgreSQL connection to the database: name of the database "
            "connection you would like to use for the upgrade."
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
                self.RUN_MIGRATIONS,
                tr("Check this box to upgrade. No action will be done otherwise"),
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

    def checkParameterValues(self, parameters, context):
        # Check if runit is checked
        run_migrations = self.parameterAsBool(parameters, self.RUN_MIGRATIONS, context)
        if not run_migrations:
            msg = tr("You must check the box to run the upgrade !")
            return False, msg

        # Check that the connection name has been configured
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        if not connection_name:
            return False, tr('You must use the "Configure plugin" alg to set the database connection name')

        connection = metadata.findConnection(connection_name)
        schema = self.parameterAsString(parameters, self.SCHEMA, context)

        # # Check that it corresponds to an existing connection
        # if connection_name not in get_postgis_connection_list():
        #     return False, tr('The configured connection name does not exists in QGIS')

        if schema in connection.schemas() and not run_migrations:
            msg = tr(
                f"Schema {schema} already exists in database ! If you REALLY "
                "want to drop and recreate it (and loose all data), check "
                "the *Overwrite* checkbox"
            )
            return False, msg

        return super(UpgradeDatabaseStructure, self).checkParameterValues(parameters, context)

    def processAlgorithm(self, parameters, context, feedback):
        # Run migration
        run_migrations = self.parameterAsBool(parameters, self.RUN_MIGRATIONS, context)
        if not run_migrations:
            msg = tr("Vous devez cocher cette case pour réaliser la mise à jour !")
            raise QgsProcessingException(msg)

        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        schema = self.parameterAsString(parameters, self.SCHEMA, context)

        connection = metadata.findConnection(connection_name)

        # Get database version
        sql = f"""
            SELECT me_version
            FROM {schema}.metadata
            WHERE me_status = 1
            ORDER BY me_version_date DESC
            LIMIT 1;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        db_version = None
        for a in data:
            db_version = int(a[0]) if a else None
        if not db_version:
            error_message = tr("No installed version found in the database !")
            raise QgsProcessingException(error_message)

        feedback.pushInfo(tr("Database structure version") + " = {}".format(db_version))

        # Get schema version
        current_version = resources.schema_version()
        feedback.pushInfo(tr("Schema version") + " = {}".format(current_version))

        # Return if nothing to do
        if db_version == current_version:
            return {
                self.OUTPUT_STATUS: 1,
                self.OUTPUT_STRING: tr(
                    " The database version already matches the plugin version. No upgrade needed."
                ),
            }

        migrations = resources.available_migrations(db_version)

        # Loop sql files and run SQL code
        for new_db_version, version, sql_file in migrations:
            with sql_file.open() as f:
                sql = f.read()
                if len(sql.strip()) == 0:
                    feedback.pushInfo(f"* {sql_file.name}  -- SKIPPED (EMPTY FILE)")
                    continue

                # Replace default SCHEMA by user defined one
                # Useful when the SQL calls functions or objects
                # prefixed by the schema
                if schema != resources.schema_name():
                    sql = sql.replace(f"{resources.schema_name()}.", f"{schema}.")
                    sql = sql.replace(f" {resources.schema_name()};", f" {schema};")

                # Add SQL database version in adresse.metadata
                feedback.pushInfo(tr("* NEW DB VERSION ") + new_db_version)
                sql += f"""
                    UPDATE {schema}.metadata
                    SET (me_version, me_version_date)
                    = ( '{new_db_version}', now()::timestamp(0) );
                """

                try:
                    connection.executeSql(sql)
                except QgsProviderConnectionException as e:
                    feedback.reportError("Error when executing file {}".format(sql_file.name))
                    connection.executeSql("ROLLBACK;")
                    raise QgsProcessingException(str(e))

                feedback.pushInfo(f"* {sql_file} -- OK !")

        # Everything is fine, we now update to the plugin version
        sql = f"""
            UPDATE {schema}.metadata
            SET (me_version, me_version_date)
            = ( '{current_version}', now()::timestamp(0) );
        """

        try:
            connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        msg = tr("*** THE DATABASE STRUCTURE HAS BEEN UPDATED ***")
        feedback.pushInfo(msg)

        return {self.OUTPUT_STATUS: 1, self.OUTPUT_STRING: msg}
