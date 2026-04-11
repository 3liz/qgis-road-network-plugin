import re

from qgis.core import (
    Qgis,
    QgsGeometry,
    QgsLocatorFilter,
    QgsLocatorResult,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)
from qgis.PyQt.QtGui import QColor

# from qgis.PyQt.QtCore import QLocale
from .processing.tools import get_connection_name


class LocatorFilter(QgsLocatorFilter):
    def __init__(self, iface):
        self.iface = iface
        super(QgsLocatorFilter, self).__init__()

    def name(self):
        return "road_network"

    def clone(self):
        return LocatorFilter(self.iface)

    def displayName(self):
        return "RoadNetwork"

    def prefix(self) -> str:
        return "rr"

    # def setEnabled(self):
    #     return True

    def fetchResults(self, search, context, feedback):
        print(f"Search: {search}")
        if len(search) < 2:
            # Let's limit the number of request sent to the server
            return

        # PostgreSQL connection
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            self.logMessage("No connection found for the current project", Qgis.MessageLevel.Critical)
            return

        # locale = QgsSettings().value("locale/userLocale", QLocale().name())
        # locale = locale.split('_')[0].lower()

        # Split search into words
        # First word is the road_code, e.g. "A6", "N7", "D100", etc.
        # Second word is the marker_code, e.g. 23 (optional)
        # Third word is the abscissa, e.g. 123.45 (optional)
        # Fourth word is the offset, e.g. 10.5 (optional)
        # Fifth word is the side, e.g. "left" or "l" or "right" or "r" (optional)
        words = search.split()
        print(words)

        # If only the road_code is provided, we aggregate all the road edges
        # If at least the road_code and the marker_code are provided,
        # we search for the corresponding point geometry
        road_code = words[0]
        if not re.match(r"(^[A-Za-z0-9À-ÿ_\-]+$)", road_code, re.UNICODE):
            return
        if len(words) == 1:
            sql = f"""
                WITH get_road AS (
                    SELECT
                        road_code,
                        CASE
                            WHEN (road_code = '{road_code}') THEN 0
                            WHEN (road_code ILIKE '{road_code}%') THEN 1
                            ELSE 2
                        END AS priority
                    FROM road_graph.roads
                    WHERE road_code ILIKE '%{road_code}%'
                    ORDER BY priority, length(road_code)
                    LIMIT 5
                )
                SELECT
                    road_code,
                    ST_AsText(road_graph.get_spatial_road(road_code)) AS wkt
                FROM get_road
            """
        else:
            marker_code = words[1]
            abscissa = words[2] if len(words) > 2 else "0"
            offset = words[3] if len(words) > 3 else "0"
            side = words[4] if len(words) > 4 else "right"
            # Check marker_code, abscissa and offset
            if not re.match(r"(^[0-9]+$)", marker_code, re.UNICODE):
                marker_code = 0
            if not re.match(r"(^[0-9\.]+$)", abscissa, re.UNICODE):
                abscissa = 0
            if not re.match(r"(^[0-9\.]+$)", offset, re.UNICODE):
                offset = 0
            # Check side
            if side.lower() in ["left", "l"]:
                side = "left"
            else:
                side = "right"
            sql = f"""
                WITH get_geom AS (
                    SELECT
                    road_graph.get_road_point_from_reference(
                        '{road_code}',
                        {marker_code},
                        {abscissa},
                        {offset},
                        '{side}'
                    ) AS point
                )
                SELECT
                    CASE
                        WHEN point->'road_code' = 'null'::jsonb
                            THEN NULL::text
                        ELSE point->>'road_code'
                    END AS road_code,
                    CASE
                        WHEN point->'geom' = 'null'::jsonb
                            THEN NULL::text
                        ELSE ST_AsText(
                            ST_GeomFromGeoJSON(
                                point->'geom'
                            )
                        )
                    END AS wkt
                FROM get_geom

            """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            self.logMessage(str(e), Qgis.MessageLevel.Critical)
            return

        if not data:
            return

        for item in data:
            result = QgsLocatorResult()
            result.filter = self
            if len(words) == 1:
                result.displayString = item[0]
            else:
                result.displayString = f"{item[0]} PR {marker_code} +{abscissa} ({offset}m {side})"
            result.userData = {
                "road_code": item[0],
                "wkt": item[1] if item[1] else None,
            }
            self.resultFetched.emit(result)

    def triggerResult(self, result):
        """Add the geometry to the canvas"""
        # Create geometry from the given wkt
        wkt = result.userData["wkt"]
        geometry = QgsGeometry.fromWkt(wkt) if wkt else None
        if not geometry:
            return

        # Flash geometry
        canvas = self.iface.mapCanvas()
        canvas.flashGeometries(
            [geometry], QgsProject.instance().crs(), QColor(255, 0, 0, 255), QColor(50, 0, 200, 200), 10, 500
        )

        # Center map
        if wkt.startswith("POINT"):
            # Center
            canvas.setCenter(geometry.asPoint())
            # Zoom to 1000 meters
            target_scale = 2500
            if canvas.scale() >= target_scale:
                canvas.zoomScale(target_scale)
        else:
            canvas.setExtent(geometry.boundingBox())
        canvas.refresh()

        # NB: We could use a QgsRubberBand to display the geometry on the canvas
