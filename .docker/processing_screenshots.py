#!/usr/bin/env python3
"""
Create screenshots of the plugin Processing algorithm dialogs for the documentation.

The dialogs are rendered by QGIS itself, without a display (Qt offscreen platform),
in a temporary QGIS profile containing a demo PostgreSQL connection.

Usage, from the repository root:

    python3 .docker/processing_screenshots.py [OUTPUT_DIR]

OUTPUT_DIR defaults to docs/media, the output file names are relative to it.
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
}

# Template layers loaded in the project, to be selected in the layer parameters
TEMPLATE_LAYERS = {
    "source_edges": ROOT / "roadnetwork" / "resources" / "import" / "source_edges.fgb",
    "source_markers": ROOT / "roadnetwork" / "resources" / "import" / "source_markers.fgb",
}

DIALOG_WIDTH = 1200
DIALOG_HEIGHT = 600


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "media"

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

    from processing.core.Processing import Processing
    from processing.gui.AlgorithmDialog import AlgorithmDialog

    Processing.initialize()

    from roadnetwork.processing.provider import Provider

    provider = Provider()
    QgsApplication.processingRegistry().addProvider(provider)

    for name, file_name in ALGORITHMS.items():
        algorithm = provider.algorithm(name)
        if algorithm is None:
            print(f"Algorithm '{name}' not found", file=sys.stderr)  # noqa: T201
            return 1

        dialog = AlgorithmDialog(algorithm.create())
        # Select the demo connection in every connection parameter
        dialog.setParameters(
            {
                param.name(): DEMO_CONNECTION_NAME
                for param in algorithm.parameterDefinitions()
                if param.type() == "providerconnection"
            }
        )
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
