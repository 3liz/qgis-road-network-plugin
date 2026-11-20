from typing import Any

from psycopg2 import connect
from psycopg2.extensions import connection as PgConnection
from psycopg2 import sql as pg_sql
from qgis.core import (
    QgsAbstractDatabaseProviderConnection,
    QgsDataSourceUri,
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

# Road graph tables merged from the editing session, in the order of the summary
GRAPH_TABLES = ("roads", "edges", "nodes", "markers")


class MergeEditingSession(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"
    editing_session_id = None

    def name(self):
        return "merge_editing_session"

    def displayName(self):
        return tr("Merge editing session data")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        return tr(
            "This algorithm will merge the data of the editing session "
            "from the sandbox into the production data, "
            "and update the managed objects impacted by the changes"
            "\n"
        )

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
        param.setHelp(tr("The connection to the database."))
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status")))
        # Add output for message
        self.addOutput(QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message")))

    def getLastCreatedEditingSessionId(
        self, status: str, parameters: dict[str, Any], context: QgsProcessingContext
    ) -> tuple:
        """Get the ID of the last editing session with status 'created'"""
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())
        sql = (
            pg_sql.SQL("""
            SELECT
                id, label,
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS')
            FROM road_graph.editing_sessions
            WHERE status = {status}
            ORDER BY created_at DESC
            LIMIT 1;
        """)
            .format(
                status=pg_sql.Literal(status),
            )
            .as_string(pg_conn)
        )
        pg_conn.close()
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
        editing_session_layers = project.mapLayersByName("editing_sessions")
        if not editing_session_layers:
            msg = tr("Cannot find the layer 'editing_sessions'. Have you opened the correct project ?")
            return False, msg
        layer = editing_session_layers[0]
        if layer.isEditable():
            msg = tr(
                "The layers are in editing mode. "
                " Please save your changes and deactivate editing beforehand !"
            )
            return False, msg

        # Get the last editing session ID with status 'edited'
        status = "edited"
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        if not editing_session:
            msg = tr(f"There is no editing session with status '{status}' in the database.\n")
            return False, msg

        return super(MergeEditingSession, self).checkParameterValues(parameters, context)

    def getGraphMergeCounts(
        self, connection: QgsAbstractDatabaseProviderConnection, pg_conn: PgConnection, session_id: int
    ) -> dict[str, dict[str, int]]:
        """Get the number of road graph objects to merge, per table and operation.

        The counts are read from the logged ids of the editing session,
        which must be done before the merge since the editing session is then deleted.

        @param connection: The QGIS PostgreSQL provider connection
        @param pg_conn: The psycopg2 connection, used to build the SQL query
        @param session_id: The id of the editing session
        @return: The counts per table and operation (I, U, D), for example {"edges": {"I": 3, "U": 1}}
        """
        sql = (
            pg_sql.SQL("""
            SELECT i.item, o.value AS op, count(*) AS nb
            FROM
                road_graph.editing_sessions AS s,
                unnest(ARRAY['roads', 'edges', 'nodes', 'markers']) AS i(item),
                jsonb_each_text(Coalesce(s.logged_ids->i.item, '{{}}'::jsonb)) AS o
            WHERE s.id = {session_id}
            GROUP BY i.item, o.value
        """)
            .format(
                session_id=pg_sql.Literal(session_id),
            )
            .as_string(pg_conn)
        )
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        # Build the counts for each table, with all the tables even without changes
        counts: dict[str, dict[str, int]] = {item: {} for item in GRAPH_TABLES}
        for item, op, nb in data:
            counts[item][op] = int(nb)

        return counts

    def getUpdatedManagedObjectsCounts(
        self, connection: QgsAbstractDatabaseProviderConnection
    ) -> list[tuple[str, str, int]]:
        """Get the number of managed objects updated by the last merge, per table.

        @param connection: The QGIS PostgreSQL provider connection
        @return: The list of the schema name, table name and number of updated objects
        """
        sql = """
            SELECT
                schema_name, table_name,
                Coalesce(cardinality(last_updated_objects_ids), 0) AS nb
            FROM road_graph.managed_objects
            ORDER BY schema_name, table_name
        """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        return [(schema_name, table_name, int(nb)) for schema_name, table_name, nb in data]

    def getMergeSummary(
        self,
        graph_counts: dict[str, dict[str, int]],
        managed_objects_counts: list[tuple[str, str, int]],
    ) -> list[str]:
        """Build the lines of the summary of the merged objects.

        @param graph_counts: The counts of the road graph objects per table and operation
        @param managed_objects_counts: The counts of the updated managed objects per table
        @return: The lines of the summary
        """
        lines = [tr("Merged objects of the road graph")]

        # One line per road graph table, with the detail per operation
        for item in GRAPH_TABLES:
            ops = graph_counts.get(item, {})
            total = sum(ops.values())
            if total == 0:
                lines.append(f"* {item}: 0")
                continue
            detail = tr("inserted {}, updated {}, deleted {}").format(
                ops.get("I", 0), ops.get("U", 0), ops.get("D", 0)
            )
            lines.append(f"* {item}: {total} ({detail})")

        # One line per managed object table
        lines.append(tr("Updated managed objects"))
        if not managed_objects_counts:
            lines.append(tr("* No managed object table"))
        for schema_name, table_name, nb in managed_objects_counts:
            lines.append(f'* "{schema_name}"."{table_name}": {nb}')

        return lines

    def reloadEdgeJoin(self):
        """
        Reload the join between edges and roads
        This allow the symbology based on road class to be refreshed
        """
        layers = QgsProject.instance().mapLayersByName("edges")
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
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())
        feedback.pushInfo(tr(f"Using connection : {connection_name}"))

        # Check for cancellation
        if feedback.isCanceled():
            pg_conn.close()
            return {}

        # Get the last editing session ID with status 'edited'
        status = "edited"
        editing_session = self.getLastCreatedEditingSessionId(status, parameters, context)
        if not editing_session:
            msg = tr(f"There is no editing session with status '{status}' in the database.\n")
            feedback.pushInfo(msg)
            pg_conn.close()
            return {}

        # Check for cancellation
        if feedback.isCanceled():
            pg_conn.close()
            return {}

        # Count the road graph objects to merge, before the editing session is deleted
        try:
            graph_counts = self.getGraphMergeCounts(connection, pg_conn, int(editing_session[0]))
        except QgsProcessingException:
            pg_conn.close()
            raise

        # Merge road_graph_editing data to road_graph schema
        feedback.pushInfo(
            tr("Merge the 'road_graph_editing' data to the production schema 'road_graph'").upper()
        )
        sql = (
            pg_sql.SQL("""
            SELECT road_graph.merge_editing_session_data({session_id}) AS result
        """)
            .format(
                session_id=pg_sql.Literal(int(editing_session[0])),
            )
            .as_string(pg_conn)
        )
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            pg_conn.close()
            raise QgsProcessingException(str(e))
        result = None
        for a in data:
            result = bool(a[0]) if a else None
        if not result:
            pg_conn.close()
            error_message = tr(
                "A problem occurred while merging the editing session data  into the 'road_graph' schema."
            )
            raise QgsProcessingException(error_message)

        pg_conn.close()
        feedback.pushInfo(
            tr(f"* The data of the editing session '{editing_session[1]}' has been successfully merged")
        )
        feedback.pushInfo(tr("Data inside the 'road_graph_editing' tables have been deleted"))
        feedback.pushInfo("")

        # Display the number of merged objects and of updated managed objects
        managed_objects_counts = self.getUpdatedManagedObjectsCounts(connection)
        summary = self.getMergeSummary(graph_counts, managed_objects_counts)
        for line in summary:
            feedback.pushInfo(line)
        feedback.pushInfo("")

        # Reload layers
        QgsProject.instance().reloadAllLayers()

        # Reload edges join with roads for symbology
        self.reloadEdgeJoin()

        # Results
        msg = tr(
            "Editing session data has been successfully merged for the given area into the road_graph schema"
        )
        msg += "\n" + "\n".join(summary)
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
