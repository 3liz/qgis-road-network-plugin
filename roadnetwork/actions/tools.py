from qgis.core import (
    Qgis,
    QgsMessageLog,
    QgsProject,
    QgsVectorLayer,
)
from qgis.utils import iface

from ..plugin_tools.i18n import tr


def display_message(title: str, message: str, level: Qgis.MessageLevel, duration: int = 5):
    """
    Display a message in the QGIS message bar.
    """
    iface.messageBar().pushMessage(title, message, level=level, duration=duration)


def display_error_message(title: str, message: str):
    """
    Display an error message in the QGIS message bar and log.
    """
    QgsMessageLog.logMessage(f"{title}: {message}", "RoadNetwork", Qgis.MessageLevel.Critical)
    display_message(title, message, Qgis.MessageLevel.Critical, 0)


def get_postgres_layer(layer_id: str, action_name: str) -> QgsVectorLayer | None:
    """
    Get a PostgreSQL vector layer from its id and check if it is valid.
    """
    layer = QgsProject.instance().mapLayer(layer_id)

    if not isinstance(layer, QgsVectorLayer):
        display_error_message(
            action_name, tr("The vector layer {} does not exist in the project").format(layer_id)
        )
        return None
    if not layer.isValid():
        display_error_message(action_name, tr("The layer {} is not valid").format(layer.name()))
        return None
    if layer.dataProvider().name() != "postgres":
        display_error_message(action_name, tr("The layer {} is not a PostgreSQL layer").format(layer.name()))
        return None

    return layer


def execute_sql_in_layer_transaction(layer: QgsVectorLayer, sql: str, action_name: str) -> bool:
    """
    Run the SQL query in the current transaction of the layer.

    The project must use the automatic transaction groups
    and the layer must be in editing mode.
    """
    transaction = layer.dataProvider().transaction()
    if transaction is None:
        display_error_message(
            action_name,
            tr("The layer {} is not in a transaction: check the project transaction mode").format(
                layer.name()
            ),
        )
        return False

    ok, error = transaction.executeSql(sql, True, "")
    if not ok:
        display_error_message(action_name, error)
        return False

    return True


def quote_literal(value: str) -> str:
    """
    Quote a text value to be used as a literal in a SQL query.
    """
    return "'{}'".format(value.replace("'", "''"))
