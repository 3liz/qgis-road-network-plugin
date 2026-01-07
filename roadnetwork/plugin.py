import os
import webbrowser

import processing

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
        self.action_toggle_dock = None
        self.action_open_help = None
        self.action_clone_data_to_editing_session = None
        self.action_merge_editing_session_data = None
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

        # Add plugin menu Open/close the dock from plugin menu
        self.action_toggle_dock = QAction(
            QIcon(str(resources_path('icons', 'toggle_dock.png'))),
            tr('Show/hide the administration dock'),
            self.iface.mainWindow()
        )
        self.action_toggle_dock.setCheckable(True)
        self.iface.addPluginToMenu(
            '&RoadNetwork',
            self.action_toggle_dock
        )
        self.action_toggle_dock.triggered.connect(self.toggle_dock)
        self.action_toggle_dock.setChecked(self.dock.isUserVisible())

        # Add help action
        self.action_open_help = QAction(
            QIcon(str(resources_path('icons', 'open_help.png'))),
            tr('Open the online help'),
            self.iface.mainWindow()
        )
        self.iface.addPluginToMenu(
            '&RoadNetwork',
            self.action_open_help
        )
        self.action_open_help.triggered.connect(self.open_help)

        # Create editing session action
        self.action_clone_data_to_editing_session = QAction(
            QIcon(str(resources_path('icons', 'clone_to_editing_session.png'))),
            tr('Clone data to the editing sandbox'),
            self.iface.mainWindow()
        )
        self.action_clone_data_to_editing_session.triggered.connect(
            self.clone_data_to_editing_session
        )

        # Merge editing session action
        self.action_merge_editing_session_data = QAction(
            QIcon(str(resources_path('icons', 'merge_editing_session.png'))),
            tr('Merge editing sandbox data'),
            self.iface.mainWindow()
        )
        self.action_merge_editing_session_data.triggered.connect(
            self.merge_editing_session_data
        )

        # Plugin toolbar
        self.toolbar = self.iface.addToolBar('&Road Network')
        self.toolbar.setObjectName("RoadNetworkToolbar")

        # Ajout des actions dans la barre
        self.toolbar.addAction(self.action_toggle_hover_tool)
        self.toolbar.addAction(self.action_clone_data_to_editing_session)
        self.toolbar.addAction(self.action_merge_editing_session_data)
        self.toolbar.addAction(self.action_toggle_dock)

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

    def toggle_dock(self):
        """ Open the dock. """
        is_open = self.dock.isUserVisible()
        self.dock.setUserVisible(not is_open)
        self.action_toggle_dock.setChecked(not is_open)

    def on_map_tool_set(self, new_map_tool, old_map_tool):
        """Detect change on map canvas map tool"""
        self.action_toggle_hover_tool.setChecked(
            (self.iface.mapCanvas().mapTool() == self.hover_map_tool)
        )

    @staticmethod
    def open_external_resource(uri, is_url=True):
        """
        Opens a file with default system app
        """
        prefix = ""
        if not is_url:
            prefix = "file://"
        webbrowser.open_new(rf"{prefix}{uri}")

    def open_help(self):
        """
        Open online help
        """
        url = "https://docs.3liz.org/qgis-road-network-plugin/"
        self.open_external_resource(url)

    def clone_data_to_editing_session(self):
        """
        Run the alg which clone data from road_graph to editing_session
        """
        # Run alg
        param = {}
        alg_name = "roadnetwork:create_editing_session"
        processing.execAlgorithmDialog(alg_name, param)

    def merge_editing_session_data(self):
        """
        Run the alg which merges the data from editing_session to road_graph
        """
        # Run alg
        param = {}
        alg_name = "roadnetwork:merge_editing_session"
        processing.execAlgorithmDialog(alg_name, param)

    def unload(self):
        """ Unload plugin """
        if self.dock:
            self.iface.removeDockWidget(self.dock)
            self.dock.deleteLater()

        if self.action_toggle_hover_tool:
            self.toolbar.removeAction(self.action_toggle_hover_tool)
            del self.action_toggle_hover_tool
        if self.action_clone_data_to_editing_session:
            self.toolbar.removeAction(self.action_clone_data_to_editing_session)
            del self.action_clone_data_to_editing_session
        if self.action_merge_editing_session_data:
            self.toolbar.removeAction(self.action_merge_editing_session_data)
            del self.action_merge_editing_session_data

        # Remove plugin menu actions
        if self.action_toggle_dock:
            self.toolbar.removeAction(self.action_toggle_dock)
            self.iface.removePluginMenu(
                '&RoadNetwork',
                self.action_toggle_dock
            )
            del self.action_toggle_dock
        if self.action_open_help:
            self.toolbar.removeAction(self.action_open_help)
            self.iface.removePluginMenu(
                '&RoadNetwork',
                self.action_open_help
            )
            del self.action_open_help

        # Remove toolbar
        self.iface.mainWindow().removeToolBar(self.toolbar)

        # Remove processing provider
        if self.provider:
            QgsApplication.processingRegistry().removeProvider(self.provider)
