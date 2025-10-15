"""Base class algorithm."""

from abc import abstractmethod

from qgis.core import QgsProcessingAlgorithm
from qgis.PyQt.QtGui import QIcon

from ..plugin_tools.resources import resources_path


class BaseProcessingAlgorithm(QgsProcessingAlgorithm):

    def createInstance(self, config={}):
        """ Virtual override

        see https://qgis.org/api/classQgsProcessingAlgorithm.html
        """
        return self.__class__()

    def flags(self):
        return super().flags() | QgsProcessingAlgorithm.FlagHideFromModeler

    def icon(self) -> QIcon:
        icon = resources_path("icons", "icon.png")
        if icon.exists():
            return QIcon(str(icon))
        else:
            return super().icon()

    def parameters_help_string(self) -> str:
        """Return a formatted help string for all parameters."""
        help_string = ""
        for param in self.parameterDefinitions():
            info = param.help()
            if info:
                help_string += f"{param.name()} : {info}\n\n"

        return help_string

    @abstractmethod
    def shortHelpString(self):
        pass
