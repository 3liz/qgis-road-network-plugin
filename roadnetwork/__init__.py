# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load class from plugin file.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    from .plugin import Plugin
    return Plugin(iface)
