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
        if len(words) == 1:
            road_code = words[0]
            sql = f"""
                SELECT
                    ST_AsText(road_graph.get_spatial_road(road_code)) AS wkt
                FROM road_graph.roads
                WHERE road_code = '{road_code}'
            """
        else:
            road_code = words[0]
            marker_code = words[1]
            abscissa = words[2] if len(words) > 2 else 0
            offset = words[3] if len(words) > 3 else 0
            side = words[4] if len(words) > 4 else "right"
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
                    )->'geom' AS geom
                )
                SELECT
                    CASE
                        WHEN geom = 'null'::jsonb THEN NULL::text
                        ELSE ST_AsText(
                            ST_GeomFromGeoJSON(
                                geom
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
                result.displayString = road_code
            else:
                result.displayString = f"{road_code} PR {marker_code} +{abscissa} ({offset}m {side})"

            result.userData = {
                "geometry_type": "point" if len(words) > 1 else "linestring",
                "wkt": item[0] if item[0] else None,
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

        # We could use a QgsRubberBand to display the geometry on the canvas
        # https://github.com/opengisch/qgis_geomapfish_locator/blob/main/geomapfish_locator/core/locator_filter.py#L185
        # https://github.com/webgeodatavore/pyqgis-samples/blob/master/gui/qgis-sample-QgsGeometryRubberBand.py
