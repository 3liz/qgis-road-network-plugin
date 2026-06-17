from psycopg2 import connect
from psycopg2 import sql as pg_sql
from qgis.core import (
    QgsAbstractDatabaseProviderConnection,
    QgsDataSourceUri,
    QgsProcessingFeedback,
    QgsProviderConnectionException,
)

from ...plugin_tools import (
    i18n,
    resources,
)
from ..base_algorithm import BaseProcessingAlgorithm


class BaseDatabaseAlgorithm(BaseProcessingAlgorithm):
    def group(self):
        return i18n.tr("Structure")

    def groupId(self):
        return f"{resources.plugin_name_normalized()}_structure"

    @staticmethod
    def vacuum_all_tables(
        connection: QgsAbstractDatabaseProviderConnection,
        feedback: QgsProcessingFeedback,
    ):
        """Execute a vacuum to recompute the feature count."""
        schema = resources.schema_name()
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())

        for table in connection.tables(schema):
            if table.tableName().startswith("v_"):
                # We can't vacuum a view
                continue

            sql = pg_sql.SQL("VACUUM ANALYSE {schema}.{table};").format(
                schema=pg_sql.Identifier(schema),
                table=pg_sql.Identifier(table.tableName()),
            ).as_string(pg_conn)
            feedback.pushDebugInfo(sql)
            try:
                connection.executeSql(sql)
            except QgsProviderConnectionException as e:
                feedback.reportError(str(e))

        pg_conn.close()
