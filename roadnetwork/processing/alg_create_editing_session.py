from typing import Any

from qgis.core import (
    QgsProcessingContext,
    QgsProcessingException,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterProviderConnection,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name


class CreateEditingSession(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"
    editing_session_id = None

    def name(self):
        return "create_editing_session"

    def displayName(self):
        return tr('Clone data to a new editing session')

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        short_help = tr(
            "This algorithm will allow to clone the data "
            " related to the editing session area into the sandbox"
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
            tr("The connection to the database.")
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

    def getLastCreatedEditingSessionId(
        self, status: str, parameters: dict[str, Any], context: QgsProcessingContext
    ) -> tuple:
        """Get the ID of the last editing session with status 'created'"""
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        sql = f"""
            SELECT
                id, label,
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS')
            FROM road_graph.editing_sessions
            WHERE status = '{status}'
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        editing_session = ()
        for a in data:
            editing_session = a if a else ()

        return editing_session

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
                " Please save your chages and deactivate editing beforehand !"
            )
            return False, msg

        # Get the editing session data for the last item
        # with status 'edited'
        session_data = self.getLastCreatedEditingSessionId('edited', parameters, context)
        if session_data:
            msg = tr(
                "There is already an editing session with status 'edited' in the database"
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
        status = 'created'
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        if not editing_session:
            msg = tr(
                f"There is no editing session with status '{status}' in the database.\n"
            )
            msg += " You must first create a new editing session polygon"
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

        # Copy the production data into the editing_session schema
        status = 'created'
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        feedback.pushInfo(
            tr(f"Copy the production data for editing session '{editing_session[1]}'").upper()
        )
        sql = f"""
            SELECT road_graph.copy_data_to_editing_session(
                {editing_session[0]}
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
            tr(
                "* The production data has been successfully copied "
                f" into editing session n°{editing_session[0]}"
            )
        )
        feedback.pushInfo("")

        # Check for cancellation
        if feedback.isCanceled():
            return {}

        # Get statistics on copied data
        feedback.pushInfo(tr("Get statistics about the copied objects").upper())
        sql = f"""
            SELECT
                jsonb_array_length(cloned_ids['edges']),
                jsonb_array_length(cloned_ids['nodes']),
                jsonb_array_length(cloned_ids['markers']),
                jsonb_array_length(cloned_ids['roads'])
            FROM road_graph.editing_sessions
            WHERE id = {editing_session[0]}
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
                f" for editing session n°{editing_session[0]}"
            )
        )
        feedback.pushInfo("")

        # Reload layers
        QgsProject.instance().reloadAllLayers()

        # Results
        msg = tr(
            "Editing sessions data has been successfully created"
            " for the given area"
        )
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
