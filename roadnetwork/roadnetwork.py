__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"

import inspect
import os
import sys

from qgis.core import QgsApplication, QgsSettings
from qgis.PyQt.QtCore import QCoreApplication, Qt, QTranslator, QUrl
from qgis.PyQt.QtGui import QDesktopServices, QIcon
from qgis.PyQt.QtWidgets import QAction

from roadnetwork.roadnetwork_dockwidget import RoadNetworkDockWidget
from roadnetwork.processing.provider import RoadNetworkProvider
from roadnetwork.qgis_plugin_tools.tools.resources import plugin_path, resources_path

cmd_folder = os.path.split(inspect.getfile(inspect.currentframe()))[0]

if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


class RoadNetworkPlugin(object):

    def __init__(self, iface):
        self.provider = None
        self.dock = None
        self.iface = iface
        self.help_action = None

        try:
            locale = QgsSettings().value('locale/userLocale', 'en')[0:2]
        except AttributeError:
            locale = 'en'
        locale_path = plugin_path('i18n', 'roadnetwork_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

    # noinspection PyPep8Naming
    def initProcessing(self):
        """Load the Processing provider."""
        self.provider = RoadNetworkProvider()
        QgsApplication.processingRegistry().addProvider(self.provider)

    # noinspection PyPep8Naming
    def initGui(self):
        self.initProcessing()
        self.dock = RoadNetworkDockWidget(self.iface)
        self.iface.addDockWidget(Qt.RightDockWidgetArea, self.dock)

        icon = QIcon(resources_path('icons', 'icon.png'))
        self.help_action = QAction(icon, 'RoadNetwork', self.iface.mainWindow())
        self.iface.pluginHelpMenu().addAction(self.help_action)
        self.help_action.triggered.connect(self.open_help)

    def unload(self):
        if self.dock:
            self.iface.removeDockWidget(self.dock)
            self.dock.deleteLater()

        if self.provider:
            QgsApplication.processingRegistry().removeProvider(self.provider)

        if self.help_action:
            self.iface.pluginHelpMenu().removeAction(self.help_action)
            del self.help_action

    @staticmethod
    def open_help():
        """ Open the online help. """
        QDesktopServices.openUrl(QUrl('https://docs.3liz.org/qgis-road-network-plugin/'))
