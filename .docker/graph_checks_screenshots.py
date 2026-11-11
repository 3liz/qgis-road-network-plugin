#!/usr/bin/env python3
"""
Create the screenshots of the documentation page docs/user-guide/graph-checks.md:

* a map of the road graph issues found by the check G01, with the context menu
  displayed by a right click on an issue,
* the dialogs used to choose the fix of an issue.

The maps are rendered by QGIS itself, without a display (Qt offscreen platform),
with the layers and styles of the administration project plugin_admin.qgs,
on the test database containing the test data.

Usage, from the repository root:

    make start-db
    # Create the database structure and import the test data
    pytest tests/test_load_structure.py -k "test_processing_create or test_reset_test_data"
    python3 .docker/graph_checks_screenshots.py [OUTPUT_DIR]
    make stop-db

or with `make graph-checks-screenshots`.

OUTPUT_DIR defaults to docs/media, the output file names are relative to it.
"""

import json
import os
import sys
import tempfile

from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
for path in ("/usr/share/qgis/python/plugins", "/usr/lib/qgis/python/plugins"):
    if Path(path).exists():
        sys.path.insert(0, path)

from qgis.core import (  # noqa: E402
    QgsApplication,
    QgsCoordinateReferenceSystem,
    QgsFeature,
    QgsMapRendererParallelJob,
    QgsMapSettings,
    QgsPointXY,
    QgsProject,
    QgsProviderRegistry,
    QgsRectangle,
)
from qgis.PyQt.QtCore import QPointF, QSize  # noqa: E402
from qgis.PyQt.QtGui import QColor, QPainter, QPen, QPolygonF  # noqa: E402
from qgis.PyQt.QtWidgets import QInputDialog, QMenu  # noqa: E402

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
CONNECTION_NAME = "road_network_test"

# Project layers displayed under the issues, from top to bottom
PROJECT_LAYERS = ("edges", "markers", "Ortho 20 cm")

IMAGE_WIDTH = 1200
IMAGE_HEIGHT = 800
JPEG_QUALITY = 85
# Half width of the map extent, in meters
HALF_WIDTH = 150


def render_map(project: QgsProject, layers: list, extent: QgsRectangle) -> tuple:
    """Render the map of the given layers.

    :param project: The QGIS project, for its transform context
    :param layers: The layers to display, from top to bottom
    :param extent: The extent of the map
    :return: The rendered image and the map settings
    """
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
    return job.renderedImage(), settings


def draw_cursor(painter: QPainter, x: float, y: float):
    """Draw a mouse cursor with its tip at the given position.

    :param painter: The painter of the image
    :param x: The horizontal position of the tip, in pixels
    :param y: The vertical position of the tip, in pixels
    """
    points = [(0, 0), (0, 22), (6, 17), (10, 26), (14, 24), (10, 16), (17, 16)]
    painter.setPen(QPen(QColor("black"), 1.5))
    painter.setBrush(QColor("white"))
    painter.drawPolygon(QPolygonF([QPointF(x + dx, y + dy) for dx, dy in points]))


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "media"

    if "PGSERVICEFILE" not in os.environ:
        service_file = Path(tempfile.mkdtemp(prefix="pg-service-")) / "pg_service.conf"
        service_file.write_text(SERVICE_DEFINITION)
        os.environ["PGSERVICEFILE"] = str(service_file)

    # Use a temporary QGIS profile, to avoid using the user connections and settings
    # The GUI is enabled to render the context menu and the dialogs
    profile_dir = tempfile.mkdtemp(prefix="qgis-profile-")
    app = QgsApplication([], True, profile_dir)
    app.initQgis()

    project = QgsProject.instance()
    if not project.read(str(PROJECT_PATH)):
        print(f"Project '{PROJECT_PATH}' cannot be read", file=sys.stderr)  # noqa: T201
        return 1

    # Connection to the test database, used by the algorithm
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.createConnection(f"service='{SERVICE_NAME}'", {})
    metadata.saveConnection(connection, CONNECTION_NAME)

    import processing

    from processing.core.Processing import Processing

    Processing.initialize()

    from roadnetwork.graph_issues import (
        check_titles,
        fix_action_labels,
        set_issue_fix_result,
        setup_issues_layer,
    )
    from roadnetwork.plugin_tools.i18n import tr
    from roadnetwork.processing.provider import Provider

    provider = Provider()
    QgsApplication.processingRegistry().addProvider(provider)

    # Issues of the check G01 in the production schema, which contains the test data
    output = processing.run(
        f"{provider.id()}:check_graph",
        {
            "CONNECTION_NAME": CONNECTION_NAME,
            "SCHEMA": 1,
            "CHECKS": "G01",
            "OUTPUT_POINTS": "TEMPORARY_OUTPUT",
            "OUTPUT_LINES": "TEMPORARY_OUTPUT",
        },
    )
    points = output["OUTPUT_POINTS"]
    lines = output["OUTPUT_LINES"]
    points.setName(tr("Road graph issues (points)"))
    project.addMapLayers([points, lines])
    setup_issues_layer(points, CONNECTION_NAME, "screenshots")
    setup_issues_layer(lines, CONNECTION_NAME, "screenshots")

    # Issue displayed: the crossing closest to the touching issue with the most crossings around it,
    # The touching issue is displayed as fixed
    issues = list(points.getFeatures())
    touching = [f for f in issues if json.loads(f["details"])["crossing_type"] == "touching"]
    crossings = [f for f in issues if json.loads(f["details"])["crossing_type"] == "crossing"]

    def crossings_around(feature: QgsFeature) -> int:
        """Number of crossings less than the half width of the map around the feature."""
        return sum(1 for c in crossings if c.geometry().distance(feature.geometry()) < HALF_WIDTH)

    fixed = max(touching, key=crossings_around)
    issue = min(crossings, key=lambda c: c.geometry().distance(fixed.geometry()))
    set_issue_fix_result(project, "screenshots", fixed["id"], "success", "")

    # Map centered between the two issues
    center = QgsPointXY(
        (fixed.geometry().asPoint().x() + issue.geometry().asPoint().x()) / 2,
        (fixed.geometry().asPoint().y() + issue.geometry().asPoint().y()) / 2,
    )
    half_height = HALF_WIDTH * IMAGE_HEIGHT / IMAGE_WIDTH
    extent = QgsRectangle(
        center.x() - HALF_WIDTH, center.y() - half_height, center.x() + HALF_WIDTH, center.y() + half_height
    )
    layers = [points, lines]
    for name in PROJECT_LAYERS:
        project_layers = project.mapLayersByName(name)
        if not project_layers or not project_layers[0].isValid():
            print(f"Layer '{name}' is not valid", file=sys.stderr)  # noqa: T201
            return 1
        layers.append(project_layers[0])
    image, settings = render_map(project, layers, extent)

    # Context menu displayed by a right click on the issue with the Identify Features tool
    menu = QMenu()
    title = menu.addAction(f"{points.name()} - {issue['check_code']} {check_titles()[issue['check_code']]}")
    title.setEnabled(False)
    menu.addSeparator()
    menu_actions = [menu.addAction(action.name()) for action in points.actions().actions()]
    menu.setActiveAction(menu_actions[1])
    menu.adjustSize()
    menu_image = menu.grab()

    # Paste the menu on the map, at the position of the click
    position = settings.mapToPixel().transform(issue.geometry().asPoint())
    painter = QPainter(image)
    painter.drawPixmap(int(position.x()) + 4, int(position.y()) + 4, menu_image)
    draw_cursor(painter, position.x(), position.y())
    painter.end()

    file_name = output_dir / "user-guide" / "graph_checks_right_click.jpg"
    file_name.parent.mkdir(parents=True, exist_ok=True)
    image.save(str(file_name), "JPG", JPEG_QUALITY)
    print(f"right click -> {file_name}")  # noqa: T201

    # Dialog to choose the fix action of the issue
    labels = fix_action_labels()
    dialog = QInputDialog()
    dialog.setWindowTitle(tr("Choose the fix of this issue"))
    dialog.setLabelText(tr("Issue {} - {}").format(issue["id"], issue["check_title"]))
    dialog.setComboBoxItems([labels["cut_edges"], labels["mark_as_bridge"]])
    dialog.setTextValue(labels["mark_as_bridge"])
    dialog.resize(500, 120)
    dialog.show()
    app.processEvents()
    file_name = output_dir / "user-guide" / "graph_checks_choose_fix.png"
    dialog.grab().save(str(file_name))
    dialog.close()
    print(f"choose fix -> {file_name}")  # noqa: T201

    # Dialog to choose the edge to mark as not connected
    road_codes = json.loads(issue["details"])["road_codes"]
    edge_ids = issue["edge_ids"].split(",")
    dialog = QInputDialog()
    dialog.setWindowTitle(tr("Choose the fix of this issue"))
    dialog.setLabelText(tr("Edges to mark as not connected"))
    dialog.setComboBoxItems(
        [tr("All the edges")]
        + [tr("Edge {} - road {}").format(edge_id, road_codes[i]) for i, edge_id in enumerate(edge_ids)]
    )
    dialog.setTextValue(tr("Edge {} - road {}").format(edge_ids[0], road_codes[0]))
    dialog.resize(500, 120)
    dialog.show()
    app.processEvents()
    file_name = output_dir / "user-guide" / "graph_checks_choose_edge.png"
    dialog.grab().save(str(file_name))
    dialog.close()
    print(f"choose edge -> {file_name}")  # noqa: T201

    app.exitQgis()
    return 0


if __name__ == "__main__":
    sys.exit(main())
