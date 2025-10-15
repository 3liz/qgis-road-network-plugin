import sys

from pathlib import Path

import pytest

from qgis.core import Qgis
from qgis.PyQt import Qt

# NOTE Remove if not using database
from .conftest_database import (  # noqa F401
    db_connection,
    db_install_version,
    db_schema,
    processing_provider,
)
from .qgis_testing import start_app

# with warnings.catch_warnings():
#    warnings.filterwarnings("ignore", category=DeprecationWarning)
#    from osgeo import gdal


def pytest_report_header(config):
    from osgeo import gdal
    message = (
        f"QGIS : {Qgis.QGIS_VERSION_INT}\n"
        f"Python GDAL : {gdal.VersionInfo('VERSION_NUM')}\n"
        f"Python : {sys.version}\n"
        f"QT : {Qt.QT_VERSION_STR}"
    )
    return message

#
# Fixtures
#


@pytest.fixture(scope="session")
def rootdir(request: pytest.FixtureRequest) -> Path:
    return request.config.rootpath


@pytest.fixture(scope="session")
def data(rootdir: Path) -> Path:
    return rootdir.joinpath("data")


def pytest_sessionstart(session: pytest.Session):
    """Start qgis application"""
    sys.path.append("/usr/share/qgis/python")  # for pyplugin installer
    start_app(session.path, False)
