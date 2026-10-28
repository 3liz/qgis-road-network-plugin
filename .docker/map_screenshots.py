#!/usr/bin/env python3
"""
Create the map screenshots of the documentation page docs/database-guide/functions.md:
a GPS track (in cyan) and the result of the function road_graph.get_snapped_line_from_track (in green).

The maps are rendered by QGIS itself, without a display (Qt offscreen platform),
with the layers and styles of the administration project plugin_admin.qgs,
on the test database containing the test data.

Usage, from the repository root:

    make start-db
    # Create the database structure and import the test data
    pytest tests/test_load_structure.py -k "test_processing_create or test_reset_test_data"
    python3 .docker/map_screenshots.py [OUTPUT_DIR]
    make stop-db

or with `make map-screenshots`.

OUTPUT_DIR defaults to docs/media, the output file names are relative to it.

The project layers use the PostgreSQL service pg_road_network_service: a temporary service file
pointing to the test database is used, unless the environment variable PGSERVICEFILE is already set.
"""

import os
import sys
import tempfile

from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

ROOT = Path(__file__).resolve().parent.parent

from qgis.core import (  # noqa: E402
    QgsApplication,
    QgsCoordinateReferenceSystem,
    QgsFeature,
    QgsGeometry,
    QgsLineSymbol,
    QgsMapRendererParallelJob,
    QgsMapSettings,
    QgsMarkerSymbol,
    QgsProject,
    QgsProviderRegistry,
    QgsRectangle,
    QgsSingleSymbolRenderer,
    QgsVectorLayer,
)
from qgis.PyQt.QtCore import QSize  # noqa: E402
from qgis.PyQt.QtGui import QColor  # noqa: E402

PROJECT_PATH = ROOT / "roadnetwork" / "resources" / "qgis" / "plugin_admin.qgs"
SERVICE_NAME = "pg_road_network_service"
# Test database started with `make start-db`
SERVICE_DEFINITION = f"""[{SERVICE_NAME}]
host=localhost
port=35432
dbname=gis
user=docker
password=docker
"""

# Project layers displayed under the GPS track, from top to bottom
PROJECT_LAYERS = ("edges", "markers", "Ortho 20 cm")

# Imprecise GPS track in EPSG:4326 (+/- 7m) going through the road D40,
# the roundabout GIR_D40_D16-1 and the road D16 (same track as in tests/test_load_structure.py)
GPS_TRACK_WKT = (
    "LINESTRING(-0.044052 49.020203,-0.043663 49.019952,-0.043503 49.019834,-0.043232 49.019758,"
    "-0.042817 49.01972,-0.04232 49.019707,-0.041969 49.019717,-0.041419 49.01973,-0.040947 49.019721,"
    "-0.040215 49.019705,-0.039546 49.019687,-0.039268 49.019656,-0.039167 49.019629,-0.038928 49.019567,"
    "-0.038934 49.019524,-0.038806 49.019484,-0.038675 49.019444,-0.038629 49.019401,-0.038761 49.019223,"
    "-0.038928 49.018876,-0.038944 49.018562,-0.038938 49.018271,-0.039021 49.018016,-0.03919 49.017737,"
    "-0.039312 49.017429,-0.039288 49.017182,-0.039202 49.017039,-0.039198 49.016874,-0.039302 49.016683,"
    "-0.039396 49.016463,-0.039369 49.016225,-0.039223 49.015851,-0.039072 49.015271,-0.039042 49.014722,"
    "-0.039105 49.014463,-0.039099 49.014186,-0.039025 49.01388,-0.038989 49.013559,-0.0391 49.013213,"
    "-0.039313 49.012978)"
)

# Output file name, relative to the output directory: view
SCREENSHOTS = {
    "database-guide/gps_track_snapping_overview.jpg": "overview",
    "database-guide/gps_track_snapping_zoom.jpg": "zoom",
}

IMAGE_WIDTH = 1200
IMAGE_HEIGHT = 800
JPEG_QUALITY = 85


def line_layer(name: str, wkt: str, color: str, width: float) -> QgsVectorLayer:
    """Memory layer containing the given line"""
    layer = QgsVectorLayer("LineString?crs=EPSG:2154", name, "memory")
    feature = QgsFeature()
    feature.setGeometry(QgsGeometry.fromWkt(wkt))
    layer.dataProvider().addFeatures([feature])
    symbol = QgsLineSymbol.createSimple(
        {
            "line_color": color,
            "line_width": str(width),
            "line_width_unit": "MM",
            "capstyle": "round",
            "joinstyle": "round",
        }
    )
    layer.setRenderer(QgsSingleSymbolRenderer(symbol))
    return layer


def points_layer(name: str, wkt: str, color: str) -> QgsVectorLayer:
    """Memory layer containing the vertices of the given line"""
    layer = QgsVectorLayer("Point?crs=EPSG:2154", name, "memory")
    features = []
    for vertex in QgsGeometry.fromWkt(wkt).vertices():
        feature = QgsFeature()
        feature.setGeometry(QgsGeometry(vertex.clone()))
        features.append(feature)
    layer.dataProvider().addFeatures(features)
    symbol = QgsMarkerSymbol.createSimple(
        {"name": "circle", "color": color, "outline_color": "0,90,90,255", "size": "2.2"}
    )
    layer.setRenderer(QgsSingleSymbolRenderer(symbol))
    return layer


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "media"

    if "PGSERVICEFILE" not in os.environ:
        service_file = Path(tempfile.mkdtemp(prefix="pg-service-")) / "pg_service.conf"
        service_file.write_text(SERVICE_DEFINITION)
        os.environ["PGSERVICEFILE"] = str(service_file)

    # Use a temporary QGIS profile, to avoid using the user connections and settings
    profile_dir = tempfile.mkdtemp(prefix="qgis-profile-")
    app = QgsApplication([], False, profile_dir)
    app.initQgis()

    project = QgsProject.instance()
    if not project.read(str(PROJECT_PATH)):
        print(f"Project '{PROJECT_PATH}' cannot be read", file=sys.stderr)  # noqa: T201
        return 1

    # GPS track and snapped line, in the road graph projection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.createConnection(f"service='{SERVICE_NAME}'", {})
    rows = connection.executeSql(
        f"""
        SELECT
            ST_AsText(ST_Transform(ST_GeomFromText('{GPS_TRACK_WKT}', 4326), 2154)),
            ST_AsText(road_graph.get_snapped_line_from_track(ST_GeomFromText('{GPS_TRACK_WKT}', 4326)))
        """
    )
    track_wkt, snapped_wkt = rows[0]
    if not snapped_wkt:
        print("The function get_snapped_line_from_track returned no geometry", file=sys.stderr)  # noqa: T201
        return 1

    # The snapped line is wider and under the track, so that the track stays visible
    layers = [
        points_layer("GPS points", track_wkt, "0,255,255,255"),
        line_layer("GPS track", track_wkt, "0,255,255,255", 0.7),
        line_layer("Snapped line", snapped_wkt, "0,200,0,255", 2.0),
    ]
    for name in PROJECT_LAYERS:
        project_layers = project.mapLayersByName(name)
        if not project_layers or not project_layers[0].isValid():
            print(f"Layer '{name}' is not valid", file=sys.stderr)  # noqa: T201
            return 1
        layers.append(project_layers[0])

    for file_name, view in SCREENSHOTS.items():
        if view == "zoom":
            # Zoom on the roundabout, between the road D40 and the road D16
            center = QgsGeometry.fromWkt(snapped_wkt).interpolate(420).asPoint()
            extent = QgsRectangle(center.x() - 180, center.y() - 120, center.x() + 180, center.y() + 120)
        else:
            extent = QgsGeometry.fromWkt(track_wkt).boundingBox()
            extent.grow(80)

        settings = QgsMapSettings()
        settings.setLayers(layers)
        settings.setDestinationCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
        settings.setOutputSize(QSize(IMAGE_WIDTH, IMAGE_HEIGHT))
        settings.setOutputDpi(96)
        settings.setBackgroundColor(QColor("white"))
        settings.setExtent(extent)
        settings.setTransformContext(project.transformContext())

        job = QgsMapRendererParallelJob(settings)
        job.start()
        job.waitForFinished()
        for error in job.errors():
            print(f"Layer '{error.layerID}': {error.message}", file=sys.stderr)  # noqa: T201

        output = output_dir / file_name
        output.parent.mkdir(parents=True, exist_ok=True)
        job.renderedImage().save(str(output), "JPG", JPEG_QUALITY)
        print(f"{view} -> {output}")  # noqa: T201

    app.exitQgis()
    return 0


if __name__ == "__main__":
    sys.exit(main())
