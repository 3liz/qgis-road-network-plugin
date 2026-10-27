#!/usr/bin/env python3
"""
Create screenshots of the plugin Processing algorithm dialogs for the documentation.

The dialogs are rendered by QGIS itself, without a display (Qt offscreen platform),
in a temporary QGIS profile containing a demo PostgreSQL connection.

Usage, from the repository root:

    python3 .docker/processing_screenshots.py [OUTPUT_DIR]
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

from qgis.core import QgsApplication, QgsProviderRegistry  # noqa: E402

DEMO_CONNECTION_NAME = "road_network_demo"
DEMO_CONNECTION_URI = "service='road_network_demo'"

# Algorithm name: output file name
ALGORITHMS = {
    "configure_plugin": "configure_plugin.png",
    "create_database_structure": "create_database_structure.png",
    "upgrade_database_structure": "upgrade_database_structure.png",
    "create_database_local_interface": "create_database_local_interface.png",
}

DIALOG_WIDTH = 1200
DIALOG_HEIGHT = 600


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "media" / "installation"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use a temporary QGIS profile, to avoid using the user connections and settings
    profile_dir = tempfile.mkdtemp(prefix="qgis-profile-")
    app = QgsApplication([], True, profile_dir)
    app.initQgis()

    # Demo PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.createConnection(DEMO_CONNECTION_URI, {})
    metadata.saveConnection(connection, DEMO_CONNECTION_NAME)

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
        dialog.grab().save(str(output))
        dialog.close()
        print(f"{name} -> {output}")  # noqa: T201

    app.exitQgis()
    return 0


if __name__ == "__main__":
    sys.exit(main())
