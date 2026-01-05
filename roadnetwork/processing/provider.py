from qgis.core import QgsExpressionContextUtils, QgsProcessingProvider
from qgis.PyQt.QtGui import QIcon

from ..plugin_tools.resources import (
    plugin_name,
    resources_path,
)
from .alg_configure_plugin import ConfigurePlugin
from .alg_create_database_local_interface import CreateDatabaseLocalInterface
from .alg_create_editing_session import CreateEditingSession
from .alg_import_data import ImportData
from .alg_merge_editing_session import MergeEditingSession
from .database import (
    CreateDatabaseStructure,
    UpgradeDatabaseStructure,
)
from .tools import provider_id


class Provider(QgsProcessingProvider):
    def unload(self):
        QgsExpressionContextUtils.setGlobalVariable(f"{self.id()}_get_database_data", "no")

    def loadAlgorithms(self):
        # Add flag used by initAlgorithm method of algs
        # so that they do not get data from database to fill in their combo boxes
        QgsExpressionContextUtils.setGlobalVariable(f"{self.id()}_get_database_data", "no")

        self.addAlgorithm(ConfigurePlugin())

        # Database
        self.addAlgorithm(CreateDatabaseStructure())
        self.addAlgorithm(UpgradeDatabaseStructure())

        # Administration
        self.addAlgorithm(CreateDatabaseLocalInterface())

        # Editing
        self.addAlgorithm(ImportData())
        self.addAlgorithm(CreateEditingSession())
        self.addAlgorithm(MergeEditingSession())

        # Put the flag back to yes
        QgsExpressionContextUtils.setGlobalVariable(f"{self.id()}_get_database_data", "yes")

    def id(self):
        return provider_id()

    def name(self):
        return plugin_name()

    def longName(self):
        return self.name()

    def icon(self):
        return QIcon(str(resources_path("icons", "icon.png")))
