from qgis.core import (
    Qgis,
    QgsExpressionContextUtils,
    QgsGeometry,
    QgsProject,
)
from qgis.gui import QgsDockWidget, QgsFilterLineEdit
from qgis.PyQt import QtWidgets
from qgis.PyQt.QtGui import (
    QColor,
    QRegularExpressionValidator,
)
from qgis.PyQt.QtCore import (
    QRegularExpression,
    pyqtSignal,
)
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

        # Connect on project load or new
        self.project = QgsProject.instance()

        # Restrict input values
        self.marker.setValidator(QRegularExpressionValidator(QRegularExpression(r'^\d+$')))
        self.abscissa.setValidator(QRegularExpressionValidator(QRegularExpression(r'^\d+(\.\d+)?$')))
        self.offset.setValidator(QRegularExpressionValidator(QRegularExpression(r'^\d+(\.\d+)?$')))
        self.side.setValidator(QRegularExpressionValidator(QRegularExpression(r'^(left|right)$')))

        # Signals/Slots
        self.button_find_point_from_references.clicked.connect(
            self.find_point_from_references
        )
        self.road_code.returnPressed.connect(self.button_find_point_from_references.click)
        self.marker.returnPressed.connect(self.button_find_point_from_references.click)
        self.abscissa.returnPressed.connect(self.button_find_point_from_references.click)
        self.offset.returnPressed.connect(self.button_find_point_from_references.click)
        self.side.returnPressed.connect(self.button_find_point_from_references.click)
        self.cumulative.returnPressed.connect(self.button_find_point_from_references.click)

    def find_point_from_references(self) -> str | None:
        """Get WKT geometry returned by the given references."""
        # Get input values
        # road_code
        item = self.findChild(QgsFilterLineEdit, 'road_code')
        road_code = 'D1'
        if item and item.value():
            road_code = item.value()
        else:
            item.setValue(str(road_code))
        # marker
        item = self.findChild(QgsFilterLineEdit, 'marker')
        marker = 0
        if item and item.value():
            marker = item.value()
        else:
            item.setValue(str(marker))
        # abscissa
        item = self.findChild(QgsFilterLineEdit, 'abscissa')
        abscissa = 0.0
        if item and item.value():
            abscissa = item.value()
        else:
            item.setValue(str(abscissa))
        # offset
        item = self.findChild(QgsFilterLineEdit, 'offset')
        offset = 0.0
        if item and item.value():
            offset = item.value()
        else:
            item.setValue(str(offset))
        # side
        item = self.findChild(QgsFilterLineEdit, 'side')
        side = 'right'
        if item and item.value():
            side = item.value()
        else:
            item.setValue(str(side))

        # Query the database
        sql = f"""
            WITH get AS (
                SELECT road_graph.get_point_from_reference(
                    '{road_code}',
                    {marker},
                    {abscissa},
                    {offset},
                    '{side}'
                )::json AS references
            )
            SELECT
                CASE
                    WHEN g.references->'geom' IS NOT NULL
                        THEN ST_AsText(
                            (g.references->>'geom')::geometry
                        )
                    ELSE 'notfound'::text
                END AS point
            FROM get AS g
        """
        # print(sql)

        project = QgsProject.instance()
        connection_name = get_connection_name(project)

        get_data = QgsExpressionContextUtils.globalScope().variable("roadnetwork_get_database_data")
        point = None
        if get_data == "yes" and connection_name in get_postgis_connection_list():
            result, _ = fetch_data_from_sql_query(connection_name, sql)
            if result:
                for a in result:
                    wkt = str(a[0])
                    if wkt and wkt.startswith('POINT'):
                        point = wkt
                    break

        # Return if no point has been found
        if not point:
            self.iface.messageBar().pushMessage(
                'RoadNetwork',
                tr('No point found for the given references'),
                Qgis.MessageLevel.Critical,
                2
            )
            return None

        # Create geometry from given point
        geometry = QgsGeometry.fromWkt(point)
        if not geometry:
            return None

        # Flash point
        canvas = self.iface.mapCanvas()
        canvas.flashGeometries(
            [geometry],
            QgsProject.instance().crs(),
            QColor(255, 0, 0, 255),
            QColor(50, 0, 200, 200),
            10,
            500
        )

        # Center map
        canvas.setExtent(geometry.boundingBox())
        canvas.refresh()

        return point

    def closeEvent(self, event):
        self.closingPlugin.emit()
        event.accept()
