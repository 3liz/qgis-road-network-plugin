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
    QgsVectorLayerJoinInfo,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name


class MergeEditingSession(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"
    editing_session_id = None

    def name(self):
        return "merge_editing_session"

    def displayName(self):
        return tr('Merge editing session data')

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
                " Please save your changes and deactivate editing beforehand !"
            )
            return False, msg

        # Get the last editing session ID with status 'edited'
        status = 'edited'
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        if not editing_session:
            msg = tr(
                f"There is no editing session with status '{status}' in the database.\n"
            )
            return False, msg

        return super(MergeEditingSession, self).checkParameterValues(parameters, context)

    def reloadEdgeJoin(self):
        """
        Reload the join between edges and roads
        This allow the symbology based on road class to be refreshed
        """
        layers = QgsProject.instance().mapLayersByName('edges')
        if not layers:
            return
        layer = layers[0]

        # Get join properties
        join = layer.vectorJoins()[0]
        join_layer = join.joinLayer()
        join_field_name = join.joinFieldName()
        join_target_field_name = join.targetFieldName()
        join_fields = join.joinFieldNamesSubset()
        join_prefix = join.prefix()

        # Remove existing join
        layer.removeJoin(join.joinLayerId())

        # Create new join
        newJoin = QgsVectorLayerJoinInfo()
        newJoin.setJoinLayer(join_layer)
        newJoin.setJoinFieldName(join_field_name)
        newJoin.setTargetFieldName(join_target_field_name)
        newJoin.setJoinFieldNamesSubset(join_fields)
        newJoin.setPrefix(join_prefix)
        newJoin.setUsingMemoryCache(True)
        layer.addJoin(newJoin)

        # Repaint layer
        layer.triggerRepaint()

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

        # Get the last editing session ID with status 'edited'
        status = 'edited'
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        if not editing_session:
            msg = tr(
                f"There is no editing session with status '{status}' in the database.\n"
            )
            feedback.pushInfo(msg)
            return {}

        # Check for cancellation
        if feedback.isCanceled():
            return {}

        # Merge editing_session data to road_graph schema
        feedback.pushInfo(
            tr("Merge the 'editing_session' data to the production schema 'road_graph'").upper()
        )
        sql = f"""
            SELECT road_graph.merge_editing_session_data(
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
                "A problem occurred while merging the editing session data "
                " into the 'road_graph' schema."
            )
            raise QgsProcessingException(error_message)

        feedback.pushInfo(
            tr(f"* The data of the editing session '{editing_session[1]}' has been successfully merged")
        )
        feedback.pushInfo(tr("Data inside the 'editing_session' tables have been deleted"))
        feedback.pushInfo("")

        # Reload layers
        QgsProject.instance().reloadAllLayers()

        # Reload edges join with roads for symbology
        self.reloadEdgeJoin()

        # Results
        msg = tr(
            "Editing session data has been successfully merged"
            " for the given area into the road_graph schema"
        )
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
