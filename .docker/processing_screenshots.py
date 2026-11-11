#!/usr/bin/env python3
"""
Create screenshots of the plugin Processing algorithm dialogs for the documentation.

The dialogs are rendered by QGIS itself, without a display (Qt offscreen platform),
in a temporary QGIS profile containing a demo PostgreSQL connection.

Usage, from the repository root:

    python3 .docker/processing_screenshots.py [OUTPUT_DIR] [ALGORITHM_NAME ...]

OUTPUT_DIR defaults to docs/media, the output file names are relative to it.
The screenshots of all the algorithms are created, unless some algorithm names are given.
"""

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

from qgis.core import QgsApplication, QgsProject, QgsProviderRegistry, QgsVectorLayer  # noqa: E402

DEMO_CONNECTION_NAME = "road_network_demo"
DEMO_CONNECTION_URI = "service='road_network_demo'"

# Algorithm name: output file name, relative to the output directory
ALGORITHMS = {
    "configure_plugin": "installation/configure_plugin.png",
    "create_database_structure": "installation/create_database_structure.png",
    "upgrade_database_structure": "installation/upgrade_database_structure.png",
    "create_database_local_interface": "installation/create_database_local_interface.png",
    "import_data": "user-guide/import_data.png",
    "create_editing_session": "user-guide/processing_create_editing_session.png",
    "merge_editing_session": "user-guide/processing_merge_editing_session.png",
    "update_managed_objects": "user-guide/processing_update_managed_objects.png",
    "get_road_sections_from_line": "user-guide/processing_get_road_sections_from_line.png",
    "check_graph": "user-guide/processing_check_graph.png",
    "fix_graph_issues": "user-guide/processing_fix_graph_issues.png",
}

# Template layers loaded in the project, to be selected in the layer parameters
TEMPLATE_LAYERS = {
    "source_edges": ROOT / "roadnetwork" / "resources" / "import" / "source_edges.fgb",
    "source_markers": ROOT / "roadnetwork" / "resources" / "import" / "source_markers.fgb",
}

# Memory layers loaded in the project, to be selected in the layer parameters: name: URI
MEMORY_LAYERS = {
    "Road graph issues (points)": "Point?crs=EPSG:2154&field=id:integer&field=check_code:string",
}

# Parameters of some algorithms, besides the connection: algorithm name: {parameter: layer name}
LAYER_PARAMETERS = {
    "fix_graph_issues": {"INPUT": "Road graph issues (points)"},
}

DIALOG_WIDTH = 1200
DIALOG_HEIGHT = 600


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "media"
    # Algorithms to process: all of them, or the given ones
    names = sys.argv[2:] or list(ALGORITHMS.keys())

    # Use a temporary QGIS profile, to avoid using the user connections and settings
    profile_dir = tempfile.mkdtemp(prefix="qgis-profile-")
    app = QgsApplication([], True, profile_dir)
    app.initQgis()

    # Demo PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.createConnection(DEMO_CONNECTION_URI, {})
    metadata.saveConnection(connection, DEMO_CONNECTION_NAME)

    for layer_name, layer_path in TEMPLATE_LAYERS.items():
        layer = QgsVectorLayer(str(layer_path), layer_name, "ogr")
        if not layer.isValid():
            print(f"Layer '{layer_path}' is not valid", file=sys.stderr)  # noqa: T201
            return 1
        QgsProject.instance().addMapLayer(layer)
    for layer_name, layer_uri in MEMORY_LAYERS.items():
        QgsProject.instance().addMapLayer(QgsVectorLayer(layer_uri, layer_name, "memory"))

    from processing.core.Processing import Processing
    from processing.gui.AlgorithmDialog import AlgorithmDialog

    Processing.initialize()

    from roadnetwork.processing.provider import Provider

    provider = Provider()
    QgsApplication.processingRegistry().addProvider(provider)

    for name in names:
        file_name = ALGORITHMS[name]
        algorithm = provider.algorithm(name)
        if algorithm is None:
            print(f"Algorithm '{name}' not found", file=sys.stderr)  # noqa: T201
            return 1

        dialog = AlgorithmDialog(algorithm.create())
        # Select the demo connection in every connection parameter
        parameters = {
            param.name(): DEMO_CONNECTION_NAME
            for param in algorithm.parameterDefinitions()
            if param.type() == "providerconnection"
        }
        # Select the given layers in the layer parameters
        for param_name, layer_name in LAYER_PARAMETERS.get(name, {}).items():
            parameters[param_name] = QgsProject.instance().mapLayersByName(layer_name)[0]
        dialog.setParameters(parameters)
        dialog.resize(DIALOG_WIDTH, DIALOG_HEIGHT)
        dialog.show()
        app.processEvents()

        output = output_dir / file_name
        output.parent.mkdir(parents=True, exist_ok=True)
        dialog.grab().save(str(output))
        dialog.close()
        print(f"{name} -> {output}")  # noqa: T201

    app.exitQgis()
    return 0


if __name__ == "__main__":
    sys.exit(main())
