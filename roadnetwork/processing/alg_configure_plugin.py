from qgis.core import (
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterProviderConnection,
    QgsProject,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name, set_connection_name


class ConfigurePlugin(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"

    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"

    def name(self):
        return "configure_plugin"

    def displayName(self):
        return tr("Configure plugin")

    def group(self):
        return tr("Configuration")

    def groupId(self):
        return f"{plugin_name_normalized()}_configuration"

    def shortHelpString(self):
        short_help = tr(
            "This algorithm will allow to configure the extension for the current "
            "QGIS project"
            "\n"
            "\n"
            "You must run this script before any other script."
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
        param.setHelp(tr("The database where the plugin structure will be installed."))
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status")))
        # Add output for message
        self.addOutput(QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message")))

    def processAlgorithm(self, parameters, context, feedback):
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)

        # Set project variable
        set_connection_name(context.project(), connection_name)
        feedback.pushInfo(tr("PostgreSQL connection to the database") + " = " + connection_name)

        msg = tr("Configuration has been saved")
        feedback.pushInfo(msg)
        status = 1

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
