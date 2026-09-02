
from psycopg2 import connect
from psycopg2 import sql as pg_sql
from qgis.core import (
    Qgis,
    QgsExpressionContextUtils,
    QgsPointXY,
    QgsProject,
)
from qgis.gui import (
    QgsMapTool,
)
from qgis.PyQt.QtCore import (
    pyqtSignal,
    QTimer,
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
    get_postgis_connection_uri_from_name,
)


class HoverMapTool(QgsMapTool):
    # Signals
    references_received = pyqtSignal(dict)

    # Active tool
    # Can be 'references' or 'canvas'
    active_tool = None

    # If we should listen to move event
    listen_move_event = False

    def __init__(self, canvas):
        QgsMapTool.__init__(self, canvas)
        self.canvas = canvas
        hover_cursor = QCursor(QPixmap(str(resources_path("icons", "hover_map_tool.png"))))
        self.cursor = hover_cursor
        self.listen_move_event = False

        # Last point where the mouse was clicked or moved
        self.last_point = None

        # The canvas event that triggered the database call
        self.canvas_event = None

        # Check if the API is currently waiting for a response
        self.is_api_busy = False

        # Timer to delay the database call when moving the mouse
        self.timer = QTimer()
        self.timer.setSingleShot(True)

        # Connect the timer's timeout the database call trigger function
        self.timer.timeout.connect(self.trigger_database_call)

        # Set tool name
        self.setToolName("road_network_hover_map_tool")

    def canvasPressEvent(self, event):
        if self.active_tool != "references":
            return

        # Always update the latest position
        self.last_point = event.originalMapPoint()

        # If the API is currently waiting for a response, drop this event
        if self.is_api_busy:
            return

        # Directly trigger the database call on click, without waiting for the timer
        self.trigger_database_call()

    def canvasMoveEvent(self, event):
        if self.active_tool != "references":
            return
        if not self.listen_move_event:
            return

        # Always update the latest position
        self.last_point = event.originalMapPoint()

        # If the API is currently waiting for a response, drop this event
        if self.is_api_busy:
            return

        # Start the timer to trigger the database call after a short delay
        # If the same function is called again before the timer expires, the timer will be restarted
        # and no database call will be made until the timer expires without being restarted.
        # This means the user will have to stop moving the mouse for 200ms before having a response
        self.timer.start(200)

    def canvasReleaseEvent(self, event):
        pass

    def activate(self):
        self.canvas.setCursor(self.cursor)
        self.active_tool = "references"
        self.activated.emit()

    def deactivate(self):
        QgsMapTool.deactivate(self)
        self.active_tool = None if not self.listen_move_event else "canvas"
        self.deactivated.emit()

    def isZoomTool(self):
        return False

    def isTransient(self):
        return False

    def isEditTool(self):
        return False

    def toggleMoveEvent(self, toggle: bool = False):
        """Sets the move event flag"""
        self.listen_move_event = toggle

        # Set project variable
        project = QgsProject.instance()
        QgsExpressionContextUtils.setProjectVariable(project, "road_network_listen_move_event", str(toggle))

        # Change the active tool
        if not toggle:
            if self.active_tool == "canvas":
                self.active_tool = None
        else:
            if not self.active_tool:
                self.active_tool = "canvas"

    def getReferenceFromLonLat(self, connection_name, lon, lat):
        """
        Query the database to get the references under the given coordinates.
        Runs entirely in a background thread.
        """
        # Run the query for both schemas and store the results
        references: dict[str, dict[str, str | int | float]] = {}
        for schema in ("editing_session", "road_graph"):
            result = ["NULL", None, None, None, None, None]
            try:
                # print(f"Fetching references for schema: {schema}, coordinates: ({lon}, {lat})")
                pg_conn = connect(get_postgis_connection_uri_from_name(connection_name).connectionInfo())
                sql = (
                    pg_sql.SQL("""
                    SELECT x.*
                    FROM
                        jsonb_to_record(
                            {schema}.get_reference_from_point(
                                ST_SetSrid(
                                    ST_MakePoint({lon}, {lat}),
                                    (
                                        SELECT srid
                                        FROM geometry_columns
                                        WHERE f_table_schema = {schema_text}
                                        AND f_table_name = 'edges'
                                        LIMIT 1
                                    )
                                ),
                                NULL
                            )
                    ) AS x (
                        road_code text, marker_code integer, abscissa real,
                        "offset" real, side text,
                        cumulative real
                    )
                """)
                    .format(
                        schema=pg_sql.Identifier(schema),
                        lon=pg_sql.Literal(lon),
                        lat=pg_sql.Literal(lat),
                        schema_text=pg_sql.Literal(schema),
                    )
                    .as_string(pg_conn)
                )
                # print(sql)
                result, error = fetch_data_from_sql_query(connection_name, sql)

            except Exception as e:
                result = ["NULL", None, None, None, None, None]
                error = str(e)
                # print(f"Error fetching references for schema {schema}: {error}")
            finally:
                pg_conn.close()

            references[schema] = {}
            references[schema]["road_code"] = result[0][0]
            references[schema]["marker"] = result[0][1]
            references[schema]["abscissa"] = result[0][2]
            references[schema]["offset"] = result[0][3]
            references[schema]["side"] = result[0][4]
            references[schema]["cumulative"] = result[0][5]
            references[schema]["error"] = error

        return references

    def onMapCanvasXYCoordinates(self, point: QgsPointXY):
        """
        Get the references at the given cursor X and Y position on the map canvas.

        This function is called when the mouse moves over the map canvas
        when the map canvas tool is not this class hoverMapTool but another QGIS tool.
        It allows to see the references under the cursor even when the user is using another tool.
        """
        if not point:
            return

        # Set the last_point
        self.last_point = point

        # If the API is currently waiting for a response, drop this event
        if self.is_api_busy:
            return

        # Start the timer to trigger the database call after a short delay
        # If the same function is called again before the timer expires, the timer will be restarted
        # and no database call will be made until the timer expires without being restarted.
        # This means the user will have to stop moving the mouse for 200ms before having a response
        self.timer.start(200)

    def trigger_database_call(self):
        """
        Emit the references at the given cursor X and Y position on the map canvas.

        References are emitted only
        if the active tool is 'references' and if the event is a click
        or if the move event is enabled.
        """
        if not self.last_point or self.is_api_busy:
            return

        # Lock the tool before starting the thread
        self.is_api_busy = True

        # Get map coordinates
        x = self.last_point.x()
        y = self.last_point.y()

        # Clean previous message
        iface.messageBar().clearWidgets()

        # Get project connection name
        project = QgsProject.instance()
        connection_name = get_connection_name(project)

        # Check connection
        if connection_name not in get_postgis_connection_list():
            iface.messageBar().pushMessage(
                "RoadNetwork",
                tr("The current project does not have a suitable road network connection"),
                Qgis.MessageLevel.Warning,
            )
            self.listen_move_event = False
            self.is_api_busy = False
            self.deactivate()

            return

        references = self.getReferenceFromLonLat(connection_name, x, y)

        # Set the emitted references
        emitted_references: dict[str, dict[str, str | int | float]] = {}

        # For each schema, check if there was an error or if the road_code is not NULL
        for schema in ("editing_session", "road_graph"):
            emitted_references[schema] = {}
            if references[schema]["error"]:
                iface.messageBar().pushMessage(
                    "RoadNetwork",
                    f"Error fetching references from {schema}: {references[schema]['²']}",
                    Qgis.MessageLevel.Critical,
                )
            if str(references[schema]["road_code"]) != "NULL":
                emitted_references[schema] = references[schema]

        # Emit references
        self.references_received.emit(references)
        self.is_api_busy = False
