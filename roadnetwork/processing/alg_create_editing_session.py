from qgis.core import (
    QgsProcessing,
    QgsProcessingException,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterProviderConnection,
    QgsProcessingParameterFeatureSource,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)
from qgis import processing
from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name, provider_id


class CreateEditingSession(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"

    def name(self):
        return "create_editing_session"

    def displayName(self):
        return tr("Create editing session")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        short_help = tr(
            "This algorithm will allow to create a new editing session "
            "\n"
        )
        return short_help

    def initAlgorithm(self, config):
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("PostgreSQL connection to the database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(
            tr("The database where the plugin structure will be installed.")
        )
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(
            QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status"))
        )
        # Add output for message
        self.addOutput(
            QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message"))
        )

    def processAlgorithm(self, parameters, context, feedback):
        # Get connection
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        feedback.pushInfo(tr(f"Using connection : {connection_name}"))

        # Check for cancellation
        if feedback.isCanceled():
            return {}

        # Get the editing session polygon
        sql = """
            SELECT
                id, label, created_at, description,
                status, unique_code, geom
            FROM road_graph.editing_sessions
            WHERE status = 'created'
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        session_data = None
        for a in data:
            session_data = int(a[0]) if a else None
        if not session_data:
            error_message = tr("There is no editing session with status 'created' in the database")
            raise QgsProcessingException(error_message)

        # Check for cancellation
        if feedback.isCanceled():
            return {}

        # Delete existing editing session
        editing_schema = 'editing_session'
        sql = f"""
            DROP SCHEMA IF EXISTS {editing_schema} CASCADE;
            CREATE SCHEMA editing_session;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        if data:
            feedback.pushInfo(f'* Previous schema {editing_schema} has been droped')

        # Fill the schema with structure
        create_structure = processing.run(
            f'{provider_id()}:create_database_structure',
            {
                'CONNECTION_NAME': connection_name,
                'OVERRIDE': True,
                'SCHEMA': editing_schema
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )
        feedback.pushInfo(f'{create_structure['OUTPUT_STRING']}')

        # Check for cancelation
        if feedback.isCanceled():
            return {}

        # Results
        msg = tr("End of the algorithm")
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
