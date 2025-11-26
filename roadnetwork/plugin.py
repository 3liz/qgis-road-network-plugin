import os

from qgis.core import QgsApplication, QgsSettings
from qgis.PyQt.QtCore import QCoreApplication, Qt, QTranslator
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction

from .dockwidget import PluginDockWidget

from .hover_map_tool import HoverMapTool

from .plugin_tools.i18n import tr
from .plugin_tools.resources import (
    plugin_path,
    resources_path,
)

from .processing.provider import Provider
from .processing.tools import plugin_name_normalized


class Plugin:
    def __init__(self, iface):
        self.provider = None
        self.dock = None
        self.iface = iface
        self.action_toggle_hover_tool = None
        self.hover_map_tool = HoverMapTool(iface.mapCanvas())

        try:
            locale = QgsSettings().value("locale/userLocale", "en")[0:2]
        except AttributeError:
            locale = "en"
        locale_path = plugin_path("i18n", f"{plugin_name_normalized()}_{locale}.qm")

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

    # noinspection PyPep8Naming
    def initProcessing(self):
        """Load the Processing provider."""
        self.provider = Provider()
        QgsApplication.processingRegistry().addProvider(self.provider)

    # noinspection PyPep8Naming
    def initGui(self):
        self.initProcessing()
        self.dock = PluginDockWidget(self.iface)
        self.iface.addDockWidget(Qt.RightDockWidgetArea, self.dock)

        # Map hover references tool
        self.action_toggle_hover_tool = QAction(
            QIcon(str(resources_path('icons', 'hover_map_tool.png'))),
            tr("Get references under the cursor"),
            self.iface.mainWindow()
        )
        self.action_toggle_hover_tool.setCheckable(True)
        self.iface.mapCanvas().mapToolSet.connect(
            self.on_map_tool_set
        )
        self.action_toggle_hover_tool.triggered.connect(
            self.toggle_hover_tool
        )

        # Plugin toolbar
        self.toolbar = self.iface.addToolBar('&RoadNetwork')
        self.toolbar.setObjectName("roadNetworkToolbar")

        # Ajout des actions dans la barre
        self.toolbar.addAction(self.action_toggle_hover_tool)

    def toggle_hover_tool(self):
        """ Toggle the map hover tool used to find references"""
        # Toggle action
        is_active = self.hover_map_tool.isActive()
        if is_active:
            # Activate identify tool
            self.hover_map_tool.deactivate()
            self.iface.actionIdentify().trigger()
        else:
            self.iface.mapCanvas().setMapTool(self.hover_map_tool)

        # self.action_toggle_hover_tool.setChecked(not is_active)

    def on_map_tool_set(self, new_map_tool, old_map_tool):
        """Detect change on map canvas map tool"""
        self.action_toggle_hover_tool.setChecked(
            (self.iface.mapCanvas().mapTool() == self.hover_map_tool)
        )

    def unload(self):
        """ Unload plugin """
        if self.dock:
            self.iface.removeDockWidget(self.dock)
            self.dock.deleteLater()

        if self.action_toggle_hover_tool:
            self.toolbar.removeAction(self.action_toggle_hover_tool)
            del self.action_toggle_hover_tool
        self.iface.mainWindow().removeToolBar(self.toolbar)

        if self.provider:
            QgsApplication.processingRegistry().removeProvider(self.provider)
