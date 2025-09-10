__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"
__revision__ = "$Format:%H$"

import os
import webbrowser

from functools import partial
from typing import Tuple

from processing import execAlgorithmDialog
from qgis.core import (
    Qgis,
    QgsExpressionContextUtils,
    QgsProject,
)
from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QPushButton

from roadnetwork.plugin_tools import available_migrations, format_version_integer
from roadnetwork.processing.algorithms.tools import (
    fetch_data_from_sql_query,
    get_postgis_connection_list,
)
from roadnetwork.qgis_plugin_tools.tools.i18n import tr
from roadnetwork.qgis_plugin_tools.tools.resources import load_ui
from roadnetwork.qgis_plugin_tools.tools.version import version

FORM_CLASS = load_ui('roadnetwork_dockwidget_base.ui')
SCHEMA='road_graph'


class RoadNetworkDockWidget(QtWidgets.QDockWidget, FORM_CLASS):

    closingPlugin = pyqtSignal()

    def __init__(self, iface, parent=None):
        """Constructor."""
        super(RoadNetworkDockWidget, self).__init__(parent)

        self.iface = iface
        self.setupUi(self)

        # Buttons directly linked to an algorithm
        self.algorithms = [
            'configure_plugin',
            'create_database_structure',
            'upgrade_database_structure',
            'create_database_local_interface',
        ]
        for alg in self.algorithms:
            button = self.findChild(QPushButton, f'button_{alg}')
            if not button:
                continue
            button.clicked.connect(partial(self.run_algorithm, alg))

        # Buttons not linked to algs
        # todo

        # Open online help
        button = self.findChild(QPushButton, 'button_online_help')
        if button:
            button.clicked.connect(self.on_line_help)

        # Connect on project load or new
        self.project = QgsProject.instance()
        self.iface.projectRead.connect(self.set_information_from_project)
        self.iface.newProjectCreated.connect(self.set_information_from_project)
        self.project.customVariablesChanged.connect(self.set_information_from_project)

        # Set information from project
        self.set_information_from_project()

    @staticmethod
    def check_database_version():
        """ Get the database version"""
        # Query the database
        sql = f'''
            SELECT me_version
            FROM {SCHEMA}.metadata
            WHERE me_status = 1
            ORDER BY me_version_date DESC
            LIMIT 1;
        '''
        project = QgsProject.instance()
        connection_name = QgsExpressionContextUtils.projectScope(project).variable('roadnetwork_connection_name')
        if not connection_name:
            connection_name = os.environ.get("roadnetwork_connection_name")
        get_data = QgsExpressionContextUtils.globalScope().variable('roadnetwork_get_database_data')
        db_version = None
        if get_data == 'yes' and connection_name in get_postgis_connection_list():
            result, _ = fetch_data_from_sql_query(connection_name, sql)
            if result:
                for a in result:
                    db_version = a[0]
                    break

        return db_version

    def set_information_from_project(self):
        """ Set project based information such as database connection name """

        # Active connection
        connection_name = QgsExpressionContextUtils.projectScope(self.project).variable('roadnetwork_connection_name')
        connection_stylesheet = 'padding: 3px;'
        connection_exists = False

        if connection_name:
            if connection_name in get_postgis_connection_list():
                connection_info = connection_name
                connection_stylesheet += "color: green;"
                connection_exists = True
            else:
                connection_info = tr(
                    f'The connection "{connection_name}" does not exist'
                )
                connection_stylesheet += "color: red;"
        else:
            connection_info = tr(
                'No connection set for this project. '
                'Use the "Configure plugin" algorithm'
            )
            connection_stylesheet += "color: red;"

        # Check database version against plugin version
        plugin_version = version()
        self.plugin_version.setText(plugin_version)
        version_comment, version_stylesheet = self.check_database_status(connection_exists)

        self.version_comment.setText(version_comment)
        self.version_comment.setStyleSheet(version_stylesheet)

        # Set project connection name and stylesheet
        self.database_connection_name.setText(connection_info)
        self.database_connection_name.setStyleSheet(connection_stylesheet)

        # Toggle activation for buttons
        all_buttons = self.algorithms
        for but in all_buttons:
            button = self.findChild(QPushButton, f'button_{but}')
            if not button:
                continue
            if but == 'configure_plugin':
                continue
            button.setEnabled(connection_exists)
            button.show()

    def check_database_status(self, connection_exists) -> Tuple[str, str]:
        """ Compare the plugin version versus the database version. """
        # First check, if there isn't any connection set.
        if not connection_exists:
            version_comment = tr('Unknown database version')
            version_stylesheet = "font-weight: bold; color: orange;"
            return version_comment, version_stylesheet

        db_version = self.check_database_version()

        # Second check, if no metadata table has been found.
        if not db_version:
            version_comment = tr(
                'The database structure version cannot be fetched from the given connection.'
            )
            version_stylesheet = "font-weight: bold; color: orange;"
            return version_comment, version_stylesheet

        self.database_version.setText(db_version)
        db_version_integer = format_version_integer(db_version)
        plugin_version_integer = format_version_integer(version())

        # Third check, if the database is in front of the plugin for their versions.
        if db_version_integer > plugin_version_integer:
            version_comment = tr(
                'The plugin version is older than the database structure version.'
                ' You need to upgrade your plugin.'
            )
            version_stylesheet = "font-weight: bold; color: orange;"
            return version_comment, version_stylesheet

        has_migrations = len(available_migrations(db_version_integer)) >= 1
        # Fourth check, if there is a migration to run
        if has_migrations:
            # db_version_integer < plugin_version_integer
            version_comment = tr(
                'The database version is older than your plugin version.'
                ' You need to run the algorithm "Upgrade database structure".'
            )
            version_stylesheet = "font-weight: bold; color: orange;"
            return version_comment, version_stylesheet

        # Finally, everything is alright
        version_comment = tr('The database is OK')
        version_stylesheet = "font-weight: bold; color: green;"
        return version_comment, version_stylesheet

    def run_algorithm(self, name):

        if name not in self.algorithms:
            self.iface.messageBar().pushMessage(
                tr("Error"),
                tr("This algorithm cannot be found") + f' {name}',
                level=Qgis.Critical
            )
            return

        # Run alg
        param = {}
        alg_name = f'roadnetwork:{name}'
        execAlgorithmDialog(alg_name, param)
        if name in ('create_database_structure', 'upgrade_database_structure'):
            self.set_information_from_project()

    def closeEvent(self, event):
        self.closingPlugin.emit()
        event.accept()

    @staticmethod
    def open_external_resource(uri, is_url=True):
        """
        Opens a file with default system app
        """
        prefix = ''
        if not is_url:
            prefix = 'file://'
        webbrowser.open_new(rf'{prefix}{uri}')

    def on_line_help(self):
        """
        Display the help on concepts
        """
        url = 'https://docs.3liz.org/qgis-road-network-plugin/'
        self.open_external_resource(url)
