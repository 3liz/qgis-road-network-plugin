from qgis.core import (
    QgsAbstractDatabaseProviderConnection,
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

        for table in connection.tables(schema):
            if table.tableName().startswith("v_"):
                # We can't vacuum a view
                continue

            sql = f"VACUUM ANALYSE {schema}.{table.tableName()};"
            feedback.pushDebugInfo(sql)
            try:
                connection.executeSql(sql)
            except QgsProviderConnectionException as e:
                feedback.reportError(str(e))
