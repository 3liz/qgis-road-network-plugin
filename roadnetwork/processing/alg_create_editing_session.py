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
    editing_session_id = None

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

    def getLastCreatedEditingSessionId(self, parameters, context) -> int:
        """Get the ID of the last editing session with status 'created'"""
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        sql = """
            SELECT
                id, label,
                created_at, updated_at
            FROM road_graph.editing_sessions
            WHERE status = 'created'
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        editing_session_id = None
        for a in data:
            editing_session_id = int(a[0]) if a else None

        return editing_session_id

    def checkParameterValues(self, parameters, context):
        # Check if an editing session is active
        # If so, cancel
        project = QgsProject.instance()
        editing_session_layers = project.mapLayersByName('editing_sessions')
        if not editing_session_layers:
            msg = tr(
                "Cannot find the layer 'editing_sessions'."
                " Have you opened the correct project ?"
            )
            return False, msg
        layer = editing_session_layers[0]
        if layer.isEditable():
            msg = tr(
                "The layers are in editing mode. "
                " Please deactivate editing beforehand !"
            )
            return False, msg

        # Check if there
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)

        # Get the editing session data
        sql = """
            SELECT
                id, label,
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS')
            FROM road_graph.editing_sessions
            WHERE status = 'edited'
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        session_data = None
        for a in data:
            session_data = a if a else None
        if session_data:
            msg = tr(
                "There is an editing session with status 'edited' in the database"
                " which has not yet been merged : \n"
                f" * id = {session_data[0]}, \n"
                f" * label is '{session_data[1]}', \n"
                f" * created at {session_data[2]}, \n"
                f" * updated at {session_data[3]}\n"
                "\n"
                " Please merge or delete this editing session beforehand."
            )
            return False, msg

        # Get the last editing session ID with status 'created'
        editing_session_id = self.getLastCreatedEditingSessionId(parameters, context)
        if not editing_session_id:
            msg = tr(
                "There is no editing session with status 'created' in the database.\n"
                " You must first create a new editing session polygon"
            )
            return False, msg

        return super(CreateEditingSession, self).checkParameterValues(parameters, context)

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

        # Drop et recreate existing editing_session schema
        feedback.pushInfo(tr("Drop and recreate the 'editing_session' schema").upper())
        editing_schema = 'editing_session'
        sql = f"""
            DROP SCHEMA IF EXISTS {editing_schema} CASCADE;
            CREATE SCHEMA editing_session;
            SELECT 1 AS test;
        """
        data = None
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        if data:
            feedback.pushInfo(tr(f'* Schema "{editing_schema}" has been dropped'))
        feedback.pushInfo("")

        # Fill the schema with structure
        feedback.pushInfo(tr(f'Create the schema "{editing_schema}" and its tables').upper())
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
        if create_structure['OUTPUT_STATUS'] == 1:
            feedback.pushInfo(tr(f'{create_structure['OUTPUT_STRING']}'))
        feedback.pushInfo(tr(f"* The schema {editing_schema} has been successfully created"))
        feedback.pushInfo("")

        # Copy the production data into the editing_session schema
        editing_session_id = self.getLastCreatedEditingSessionId(parameters, context)
        feedback.pushInfo(tr(f"Copy the production data for editing session n°{editing_session_id}").upper())
        sql = f"""
            SELECT road_graph.copy_data_to_editing_session(
                {editing_session_id}
            ) AS result
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        result = None
        for a in data:
            result = bool(a[0]) if a else None
        if not result:
            error_message = tr(
                "A problem occurred while copying the production data "
                " into the 'editing_session' schema."
            )
            raise QgsProcessingException(error_message)

        feedback.pushInfo(
            tr(f"* The production data has been successfully copied into editing session n°{editing_session_id}")
        )
        feedback.pushInfo("")

        # Check for cancellation
        if feedback.isCanceled():
            return {}

        # Get statistics on copied data
        feedback.pushInfo(tr(f"Get statistics about the copied objects").upper())
        sql = f"""
            SELECT
                jsonb_array_length(cloned_ids['edges']),
                jsonb_array_length(cloned_ids['nodes']),
                jsonb_array_length(cloned_ids['markers']),
                jsonb_array_length(cloned_ids['roads'])
            FROM road_graph.editing_sessions
            WHERE id = {editing_session_id}
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        stats = None
        for a in data:
            stats = a if a else None
        if not stats:
            error_message = tr(
                "A problem occurred while getting statistics on copied objects "
                " from the 'editing_session' schema."
            )
            raise QgsProcessingException(error_message)

        feedback.pushInfo(
            tr(
                "Number of copied objects:\n"
                f"* {stats[0]} edges\n"
                f"* {stats[1]} nodes\n"
                f"* {stats[2]} markers\n"
                f"* {stats[3]} roads\n"
                f" for editing session n°{editing_session_id}"
            )
        )
        feedback.pushInfo("")

        # Results
        msg = tr("Editing sessions data has been successfully created")
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
