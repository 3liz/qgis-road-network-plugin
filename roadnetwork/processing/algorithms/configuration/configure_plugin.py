__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"

from qgis.core import (
    QgsExpressionContextUtils,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
    QgsProcessingParameterProviderConnection,
    QgsProject,
)

from roadnetwork.qgis_plugin_tools.tools.algorithm_processing import (
    BaseProcessingAlgorithm,
)
from roadnetwork.qgis_plugin_tools.tools.i18n import tr


class ConfigurePlugin(BaseProcessingAlgorithm):

    CONNECTION_NAME = 'CONNECTION_NAME'

    OUTPUT_STATUS = 'OUTPUT_STATUS'
    OUTPUT_STRING = 'OUTPUT_STRING'

    def name(self):
        return 'configure_plugin'

    def displayName(self):
        return tr('Configure plugin')

    def group(self):
        return tr('Configuration')

    def groupId(self):
        return 'roadnetwork_configuration'

    def shortHelpString(self):
        short_help = tr(
            'This algorithm will allow to configure the extension for the current QGIS project'
            '\n'
            '\n'
            'You must run this script before any other script.'
            '\n'
            '\n'
            '* PostgreSQL connection to the database: name of the database connection you would like to use for the current QGIS project. This connection will be used for the other algorithms.'
        )
        return short_help

    def initAlgorithm(self, config):
        project = QgsProject.instance()
        connection_name = QgsExpressionContextUtils.projectScope(project).variable('roadnetwork_connection_name')
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr('PostgreSQL connection to the database'),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(tr("The database where the plugin structure will be installed."))
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(
            QgsProcessingOutputNumber(
                self.OUTPUT_STATUS,
                tr('Output status')
            )
        )
        # Add output for message
        self.addOutput(
            QgsProcessingOutputString(
                self.OUTPUT_STRING,
                tr('Output message')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        connection_name = self.parameterAsConnectionName(
            parameters, self.CONNECTION_NAME, context
        )

        # Set project variable
        QgsExpressionContextUtils.setProjectVariable(context.project(), 'roadnetwork_connection_name', connection_name)
        feedback.pushInfo(tr('PostgreSQL connection to the database') + ' = ' + connection_name)

        msg = tr('Configuration has been saved')
        feedback.pushInfo(msg)
        status = 1

        return {
            self.OUTPUT_STATUS: status,
            self.OUTPUT_STRING: msg
        }
