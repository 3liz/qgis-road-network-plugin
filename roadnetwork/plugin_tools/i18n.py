#
# Translation
#

from qgis.PyQt.QtWidgets import QApplication


def tr(text: str, context: str = "@default") -> str:
    return QApplication.translate(context, text)
