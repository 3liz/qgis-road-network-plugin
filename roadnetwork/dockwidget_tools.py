from qgis.core import (
    QgsExpressionContextUtils,
    QgsProject,
)
from qgis.gui import QgsDockWidget
from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import pyqtSignal

from .plugin_tools.i18n import tr
from .plugin_tools.resources import (
    load_ui,
)
from .processing.tools import (
    fetch_data_from_sql_query,
    get_connection_name,
    get_postgis_connection_list,
)

FORM_CLASS = load_ui("dockwidget_tools.ui")


class ToolsDockWidget(QgsDockWidget, QtWidgets.QDockWidget, FORM_CLASS):  # type: ignore [misc, valid-type]
    closingPlugin = pyqtSignal()

    def __init__(self, iface, parent=None):
        """Constructor."""
        super(ToolsDockWidget, self).__init__(parent)

        self.iface = iface
        self.setupUi(self)

        # Buttons not linked to algs
        # todo

        # Connect on project load or new
        self.project = QgsProject.instance()

    def get_point_from_references(
        self,
        road_code: str,
        marker_code: int = 0,
        abscissa: float = 0,
        offset: float = 0,
        side: str = 'right'
    ):
        """Get WKT geometry returned by the given references."""
        # Query the database
        sql = f"""
            SELECT 1
        """
        project = QgsProject.instance()
        connection_name = get_connection_name(project)

        get_data = QgsExpressionContextUtils.globalScope().variable("roadnetwork_get_database_data")
        point = None
        if get_data == "yes" and connection_name in get_postgis_connection_list():
            result, _ = fetch_data_from_sql_query(connection_name, sql)
            if result:
                for a in result:
                    point = int(a[0])
                    break

        return point

    def closeEvent(self, event):
        self.closingPlugin.emit()
        event.accept()
