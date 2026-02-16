import time

from qgis.core import (
    Qgis,
    QgsPointXY,
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

    # Active tool
    # Can be 'maptool' or 'canvas'
    active_tool = None

    # If we should listen to move event
    listen_move_event = False

    # Number of milliseconds since last emission of references, used to limit the number of emissions
    minimum_time_between_emissions_ms = 150
    emitted_since_ms = 0

    def __init__(self, canvas):
        QgsMapTool.__init__(self, canvas)
        self.canvas = canvas
        hover_cursor = QCursor(QPixmap(
            str(resources_path('icons', 'hover_map_tool.png'))
        ))
        self.cursor = hover_cursor
        self.listen_move_event = False

    def canvasPressEvent(self, event):
        if self.active_tool == 'maptool':
            self.emitMapCursorReferences(event.originalMapPoint(), 'click')
        pass

    def canvasMoveEvent(self, event):
        if self.listen_move_event and self.active_tool == 'maptool':
            self.emitMapCursorReferences(event.originalMapPoint(), 'move')
        pass

    def canvasReleaseEvent(self, event):
        pass

    def activate(self):
        self.canvas.setCursor(self.cursor)
        self.active_tool = 'maptool'
        self.activated.emit()

    def deactivate(self):
        QgsMapTool.deactivate(self)
        self.active_tool = None if not self.listen_move_event else 'canvas'
        self.deactivated.emit()

    def isZoomTool(self):
        return False

    def isTransient(self):
        return False

    def isEditTool(self):
        return False

    def toggleMoveEvent(self, toggle: bool = False):
        """ Sets the move event flag"""
        self.listen_move_event = toggle
        # Change the active tool
        if not toggle:
            if self.active_tool == 'canvas':
                self.active_tool = None
        else:
            if not self.active_tool:
                self.active_tool = 'canvas'

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

    def emitMapCursorReferences(self, map_position: QgsPointXY, event_name: str = 'move'):
        """
        Emit the references at the given cursor X and Y position on the map canvas.

        References are emitted only
        if the active tool is 'maptool' and if the event is a click
        or if the move event is enabled.
        """
        # Do nothing if conditions are not met
        if not self.active_tool:
            return
        if not self.listen_move_event:
            if self.active_tool == 'canvas':
                return
            if event_name != 'click':
                return

        # Very basic limitation of number of time the function is called
        current_time_ms = int(time.perf_counter_ns() / 1000000)
        if current_time_ms - self.emitted_since_ms < self.minimum_time_between_emissions_ms:
            return
        self.emitted_since_ms = current_time_ms

        # Get map coordinates
        x = map_position.x()
        y = map_position.y()

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
            self.listen_move_event = False
            self.deactivate()

            return

        # Display references or error message
        # editing_sessions
        emitted_references = {}
        for schema in ('editing_session', 'road_graph'):
            references, error = self.getReferenceFromLonLat(
                connection_name,
                schema,
                x,
                y
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

        # Emit references
        self.references_received.emit(emitted_references)
