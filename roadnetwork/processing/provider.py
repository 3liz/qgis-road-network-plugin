__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"
__revision__ = "$Format:%H$"

from qgis.core import QgsExpressionContextUtils, QgsProcessingProvider
from qgis.PyQt.QtGui import QIcon

from roadnetwork.plugin_tools import resources_path
from roadnetwork.processing.algorithms.configuration.configure_plugin import (
    ConfigurePlugin,
)
from roadnetwork.processing.algorithms.create_database_local_interface import CreateDatabaseLocalInterface
from roadnetwork.processing.algorithms.database.create import CreateDatabaseStructure
from roadnetwork.processing.algorithms.database.upgrade import UpgradeDatabaseStructure

from roadnetwork.qgis_plugin_tools.tools.i18n import tr


class RoadNetworkProvider(QgsProcessingProvider):

    def unload(self):
        QgsExpressionContextUtils.setGlobalVariable('roadnetwork_get_database_data', 'no')

    def loadAlgorithms(self):

        # Add flag used by initAlgorithm method of algs
        # so that they do not get data from database to fill in their combo boxes
        QgsExpressionContextUtils.setGlobalVariable('roadnetwork_get_database_data', 'no')

        self.addAlgorithm(ConfigurePlugin())

        # Database
        self.addAlgorithm(CreateDatabaseStructure())
        self.addAlgorithm(UpgradeDatabaseStructure())

        self.addAlgorithm(CreateDatabaseLocalInterface())

        # Put the flag back to yes
        QgsExpressionContextUtils.setGlobalVariable('roadnetwork_get_database_data', 'yes')

    def id(self):  # NOQA: A003
        return 'roadnetwork'

    def name(self):
        return tr('RoadNetwork')

    def longName(self):
        return self.name()

    def icon(self):
        return QIcon(str(resources_path('icons', 'icon.png')))
