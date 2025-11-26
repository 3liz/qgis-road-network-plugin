from qgis.core import (
    Qgis,
    QgsProject,
)
from qgis.gui import (
    QgsMapTool,
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
    def __init__(self, canvas):
        QgsMapTool.__init__(self, canvas)
        self.canvas = canvas
        hover_cursor = QCursor(QPixmap(
            str(resources_path('icons', 'hover_map_tool_32.png'))
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

    def getReferenceFromLonLat(self, connection_name, lon, lat):
        """
        Query the database to get the references under the given coordinates
        """
        sql = f"""
            SELECT x.*
            FROM
                jsonb_to_record(
                    road_graph.get_reference_from_point(
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

        # Display references or error message
        if connection_name in get_postgis_connection_list():
            references, error = self.getReferenceFromLonLat(
                connection_name,
                point.x(),
                point.y()
            )
            message = ''
            if str(references[0][0]) != 'NULL':
                message += tr(f"""{references[0][0]} PR {references[0][1]} + {references[0][2]}
                decal {references[0][3]} {references[0][4]}
                cumul {references[0][5]}
                """)
                iface.messageBar().pushMessage(
                    'RoadNetwork',
                    message,
                    Qgis.MessageLevel.Success
                )
            if error:
                iface.messageBar().pushMessage(
                    'RoadNetwork',
                    error,
                    Qgis.MessageLevel.Critical
                )
        else:
            iface.messageBar().pushMessage(
                'RoadNetwork',
                tr('The current project does not have a suitable road network connection'),
                Qgis.MessageLevel.Warning
            )
