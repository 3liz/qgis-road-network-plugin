from qgis.core import (
    QgsProcessing,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterProviderConnection,
    QgsProcessingParameterFeatureSource,
    QgsProject,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name


class ImportData(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    INPUT_NODES = "INPUT_NODES"
    INPUT_ROADS = "INPUT_ROADS"
    INPUT_EDGES = "INPUT_EDGES"
    INPUT_MARKERS = "INPUT_MARKERS"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"

    def name(self):
        return "import_data"

    def displayName(self):
        return tr("Import data")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        short_help = tr(
            "This algorithm will allow to import data into the database "
            "\n"
            "\n"
            "* PostgreSQL connection to the database: name of the database "
            "connection you would like to use for the current QGIS project. "
            "This connection will be used for the other algorithms."
        )
        return short_help

    def initAlgorithm(self, config):
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("PostgreSQL connection to the database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(
            tr("The database where the plugin structure will be installed.")
        )
        self.addParameter(param)

        # Road source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_NODES,
            tr("Nodes source data"),
            [QgsProcessing.SourceType.TypeVectorPoint],
        )
        param.setHelp(tr("Nodes source data"))
        self.addParameter(param)

        # Edges source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_ROADS,
            tr("Roads source data"),
            [QgsProcessing.SourceType.TypeVector],
        )
        param.setHelp(tr("Roads source data"))
        self.addParameter(param)

        # Edges source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_EDGES,
            tr("Edges source data"),
            [QgsProcessing.SourceType.TypeVectorLine],
        )
        param.setHelp(tr("Edges source data"))
        self.addParameter(param)

        # Markers source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_MARKERS,
            tr("Markers source data"),
            [QgsProcessing.SourceType.TypeVectorPoint],
        )
        param.setHelp(tr("Markers source data"))
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(
            QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status"))
        )
        # Add output for message
        self.addOutput(
            QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message"))
        )

    def processAlgorithm(self, parameters, context, feedback):
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )

        # Set project variable
        feedback.pushInfo(tr(f"Using connection : {connection_name}"))

        msg = tr("Configuration has been saved")
        feedback.pushInfo(msg)
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
