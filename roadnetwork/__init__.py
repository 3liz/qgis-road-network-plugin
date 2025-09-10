__copyright__ = "Copyright 2025, 3Liz"
__license__ = "GPL version 3"
__email__ = "info@3liz.org"
__revision__ = "$Format:%H$"


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load class from plugin file.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    from roadnetwork.roadnetwork import RoadNetworkPlugin
    return RoadNetworkPlugin(iface)
