from qgis.core import (
    Qgis,
    QgsProject,
)
from qgis.gui import (
    QgsMapTool,
)
from qgis.PyQt.QtCore import (
    pyqtSignal,
)
from qgis.PyQt.QtGui import (
    QCursor,
    QPixmap,
)
from qgis.utils import iface

from .plugin_tools.i18n import tr
from .plugin_tools.resources import resources_path
from .processing.tools import (
    fetch_data_from_sql_query,
    get_connection_name,
    get_postgis_connection_list,
)


class HoverMapTool(QgsMapTool):

    # Signals
    references_received = pyqtSignal(dict)

    def __init__(self, canvas):
        QgsMapTool.__init__(self, canvas)
        self.canvas = canvas
        hover_cursor = QCursor(QPixmap(
            str(resources_path('icons', 'hover_map_tool.png'))
        ))
        self.cursor = hover_cursor

    def canvasPressEvent(self, event):
        pass

    def canvasMoveEvent(self, event):
        self.displayReferences(event)

    def canvasReleaseEvent(self, event):
        self.displayReferences(event)

    def activate(self):
        self.canvas.setCursor(self.cursor)
        self.activated.emit()

    def deactivate(self):
        QgsMapTool.deactivate(self)
        self.deactivated.emit()

    def isZoomTool(self):
        return False

    def isTransient(self):
        return False

    def isEditTool(self):
        return False

    def getReferenceFromLonLat(self, connection_name, schema, lon, lat):
        """
        Query the database to get the references under the given coordinates
        """
        sql = f"""
            SELECT x.*
            FROM
                jsonb_to_record(
                    "{schema}".get_reference_from_point(
                        ST_SetSrid(
                            ST_MakePoint({lon}, {lat}),
                            2154
                        ),
                        NULL
                    )
            ) AS x (
                road_code text, marker_code integer, abscissa real,
                "offset" real, side text,
                cumulative real
            )
        """
        # print(sql)
        result, error = fetch_data_from_sql_query(connection_name, sql)

        return result, error

    def displayReferences(self, event):
        """ Show the references or an error message"""
        # Get canvas coordinates
        x = event.pos().x()
        y = event.pos().y()

        # Get map coordinates
        point = self.canvas.getCoordinateTransform().toMapCoordinates(x, y)

        # Clean previous message
        iface.messageBar().clearWidgets()

        # Get project connection name
        project = QgsProject.instance()
        connection_name = get_connection_name(project)

        # Check connection
        if connection_name not in get_postgis_connection_list():
            iface.messageBar().pushMessage(
                'RoadNetwork',
                tr('The current project does not have a suitable road network connection'),
                Qgis.MessageLevel.Warning
            )
            return

        # Display references or error message
        # editing_sessions
        emitted_references = {}
        for schema in ('editing_session', 'road_graph'):
            references, error = self.getReferenceFromLonLat(
                connection_name,
                schema,
                point.x(),
                point.y()
            )
            emitted_references[schema] = {}
            if str(references[0][0]) != 'NULL':
                emitted_references[schema]['road_code'] = references[0][0]
                emitted_references[schema]['marker'] = references[0][1]
                emitted_references[schema]['abscissa'] = references[0][2]
                emitted_references[schema]['offset'] = references[0][3]
                emitted_references[schema]['side'] = references[0][4]
                emitted_references[schema]['cumulative'] = references[0][5]

            if error and schema == 'road_graph':
                iface.messageBar().pushMessage(
                    'RoadNetwork',
                    error,
                    Qgis.MessageLevel.Critical
                )
        self.references_received.emit(emitted_references)
