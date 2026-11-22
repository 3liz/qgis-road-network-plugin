import json
import re
import shutil

from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

from qgis.core import (
    Qgis,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsCoordinateTransformContext,
    QgsDataSourceUri,
    QgsExpressionContextUtils,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsRectangle,
)
from qgis.PyQt.QtCore import QVariant
from qgis.PyQt.QtXml import QDomDocument, QDomElement

from ..plugin_tools.resources import (
    plugin_name_normalized,
    plugin_path,
    srid_value,
)

CONNECTION_NAME_CONTEXT_VAR = f"{plugin_name_normalized()}_connection_name"


def provider_id() -> str:
    return plugin_name_normalized()


def set_connection_name(project: QgsProject, connection_name: str):
    QgsExpressionContextUtils.setProjectVariable(project, CONNECTION_NAME_CONTEXT_VAR, connection_name)


def get_connection_name(project: QgsProject) -> str:
    return QgsExpressionContextUtils.projectScope(project).variable(
        CONNECTION_NAME_CONTEXT_VAR,
    )


def get_postgis_connection_list():
    """Get a list of the PostGIS connection names"""
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    postgres_connections = metadata.connections()
    return postgres_connections.keys()


def get_postgis_connection_uri_from_name(connection_name: str) -> Optional[QgsDataSourceUri]:
    """
    Return a QgsDatasourceUri from a PostgreSQL connection name
    """
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.findConnection(connection_name)
    if not connection:
        return None

    return QgsDataSourceUri(connection.uri())


def fetch_data_from_sql_query(
    connection_name: str, sql: str
) -> Union[Tuple[Any, None], Tuple[List[Any], str]]:
    """Execute SQL and return the result."""
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.findConnection(connection_name)

    try:
        result = connection.executeSql(sql)
        return result, None
    except QgsProviderConnectionException as e:
        return [], str(e)


def getVersionInteger(f):
    """
    Transform "0.1.2" into "000102"
    Transform "10.9.12" into "100912"
    to allow comparing versions
    and sorting the upgrade files
    """
    return "".join([a.zfill(2) for a in f.strip().split(".")])


def get_layer_extent(
    connection_name: str, uri: QgsDataSourceUri, crs: QgsCoordinateReferenceSystem
) -> Optional[QgsRectangle]:
    """Get the extent of the geometries of a PostgreSQL table, in the given coordinate reference system.

    :param connection_name: The name of the PostgreSQL connection
    :param uri: The data source of the layer: schema, table and geometry column
    :param crs: The coordinate reference system of the extent
    :return: The extent, or None if the table has no geometry or cannot be read
    """
    # The identifiers are quoted, and the SRID is an integer
    sql = (
        "SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM ("  # nosec B608
        "SELECT ST_Transform(ST_SetSRID(ST_Extent(\"{column}\")::geometry, Find_SRID('{schema}', "
        "'{table}', '{column}')), {srid}) AS e "
        'FROM "{schema}"."{table}"'
        ") AS s"
    ).format(
        schema=uri.schema().replace('"', "").replace("'", ""),
        table=uri.table().replace('"', "").replace("'", ""),
        column=uri.geometryColumn().replace('"', "").replace("'", ""),
        srid=int(crs.postgisSrid()),
    )
    data, error = fetch_data_from_sql_query(connection_name, sql)
    # An empty table returns NULL values
    if error or not data or any(v is None or (isinstance(v, QVariant) and v.isNull()) for v in data[0]):
        return None
    return QgsRectangle(*[float(v) for v in data[0]])


def set_extent_element(element: QDomElement, extent: QgsRectangle):
    """Set the coordinates of an extent element of a QGIS project, with xmin, ymin, xmax and ymax children.

    :param element: The extent element, for example the extent of the map canvas or of a layer
    :param extent: The extent
    """
    for name, value in (
        ("xmin", extent.xMinimum()),
        ("ymin", extent.yMinimum()),
        ("xmax", extent.xMaximum()),
        ("ymax", extent.yMaximum()),
    ):
        child = element.firstChildElement(name)
        if child.isNull():
            child = element.ownerDocument().createElement(name)
            element.appendChild(child)
        while child.hasChildNodes():
            child.removeChild(child.firstChild())
        child.appendChild(element.ownerDocument().createTextNode(repr(value)))


def update_project_crs_and_extents(
    document: QDomDocument,
    connection_name: str,
    template_crs: QgsCoordinateReferenceSystem,
    crs: QgsCoordinateReferenceSystem,
) -> QgsRectangle:
    """Set the coordinate reference system and the extents of the administration project.

    The coordinate reference system of the template is replaced by the given one for the project
    and for the PostgreSQL layers. The other layers, for example the base maps, keep their own system.
    The extents of the PostgreSQL layers are calculated from their tables, and the extent of the map
    from the edges of the road graph, or else from the area of use of the coordinate reference system.

    :param document: The XML document of the project, modified in place
    :param connection_name: The name of the PostgreSQL connection
    :param template_crs: The coordinate reference system of the template project
    :param crs: The coordinate reference system of the road graph
    :return: The extent of the map
    """
    to_wgs84 = QgsCoordinateTransform(
        crs, QgsCoordinateReferenceSystem("EPSG:4326"), QgsCoordinateTransformContext()
    )

    # Extent of the map: the edges of the road graph, else the area of use of the system
    uri = QgsDataSourceUri()
    uri.setDataSource("road_graph", "edges", "geom")
    map_extent = get_layer_extent(connection_name, uri, crs)
    if map_extent is None or map_extent.isEmpty():
        map_extent = to_wgs84.transformBoundingBox(crs.bounds(), Qgis.TransformDirection.Reverse)
    else:
        map_extent.scale(1.05)

    # Layers: data source, system and extents of the PostgreSQL layers
    layers = document.elementsByTagName("maplayer")
    for index in range(layers.count()):
        layer = layers.at(index).toElement()
        if layer.firstChildElement("provider").text() != "postgres":
            continue
        layer_uri = QgsDataSourceUri(layer.firstChildElement("datasource").text())
        if not layer_uri.geometryColumn():
            continue
        extent = get_layer_extent(connection_name, layer_uri, crs) or map_extent
        set_extent_element(layer.firstChildElement("extent"), extent)
        wgs84_extent = layer.firstChildElement("wgs84extent")
        if not wgs84_extent.isNull():
            set_extent_element(wgs84_extent, to_wgs84.transformBoundingBox(extent))
        # Extent of the metadata of the layer
        spatials = layer.elementsByTagName("spatial")
        for spatial_index in range(spatials.count()):
            spatial = spatials.at(spatial_index).toElement()
            spatial.setAttribute("crs", crs.authid())
            for name, value in (
                ("minx", extent.xMinimum()),
                ("miny", extent.yMinimum()),
                ("maxx", extent.xMaximum()),
                ("maxy", extent.yMaximum()),
            ):
                spatial.setAttribute(name, repr(value))

    # Systems of the project and of the PostgreSQL layers: the systems of the template are replaced.
    # The list is copied first, since the replaced elements are removed from the live list
    systems = document.elementsByTagName("spatialrefsys")
    elements = [systems.at(index).toElement() for index in range(systems.count())]
    for element in elements:
        if element.firstChildElement("authid").text() != template_crs.authid():
            continue
        # Keep the systems of the other layers, for example a base map in Lambert 93
        parent = element.parentNode()
        while not parent.isNull() and parent.nodeName() != "maplayer":
            parent = parent.parentNode()
        if not parent.isNull() and parent.toElement().firstChildElement("provider").text() != "postgres":
            continue
        container = document.createElement("container")
        crs.writeXml(container, document)
        element.parentNode().replaceChild(container.firstChildElement("spatialrefsys"), element)

    # Extent of the map canvas, and default extent of the project
    canvas = document.elementsByTagName("mapcanvas")
    for index in range(canvas.count()):
        set_extent_element(canvas.at(index).toElement().firstChildElement("extent"), map_extent)
    view_extents = document.elementsByTagName("DefaultViewExtent")
    for index in range(view_extents.count()):
        view_extent = view_extents.at(index).toElement()
        view_extent.setAttribute("xmin", repr(map_extent.xMinimum()))
        view_extent.setAttribute("ymin", repr(map_extent.yMinimum()))
        view_extent.setAttribute("xmax", repr(map_extent.xMaximum()))
        view_extent.setAttribute("ymax", repr(map_extent.yMaximum()))
    return map_extent


def update_lizmap_config(
    config_file: Path,
    template_crs: QgsCoordinateReferenceSystem,
    crs: QgsCoordinateReferenceSystem,
    extent: QgsRectangle,
):
    """Set the coordinate reference system and the extents of the Lizmap configuration of the project.

    :param config_file: The Lizmap configuration file, modified in place
    :param template_crs: The coordinate reference system of the template project
    :param crs: The coordinate reference system of the road graph
    :param extent: The extent of the map
    """
    config = json.loads(config_file.read_text(encoding="utf-8"))
    coordinates = [extent.xMinimum(), extent.yMinimum(), extent.xMaximum(), extent.yMaximum()]
    options = config.get("options", {})
    options["projection"] = {"proj4": crs.toProj4(), "ref": crs.authid()}
    options["bbox"] = [repr(c) for c in coordinates]
    options["initialExtent"] = coordinates
    # The vector layers in the system of the template: the base maps keep their own system
    for layer in config.get("layers", {}).values():
        if layer.get("crs") == template_crs.authid() and layer.get("geometryType"):
            layer["crs"] = crs.authid()
            layer["extent"] = coordinates
    config_file.write_text(json.dumps(config, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")


def createAdministrationProjectFromTemplate(
    connection_name: str,
    crs: QgsCoordinateReferenceSystem,
    project_file_path: str,
) -> bool:
    """Create the administration project from the template plugin_admin.qgs, for a PostgreSQL connection.

    The data sources use the connection, and the coordinate reference system of the road graph.
    The extents of the project and of its layers are calculated from the database.
    The Lizmap configuration of the template is copied and updated the same way.

    :param connection_name: The name of the PostgreSQL connection
    :param crs: The coordinate reference system of the road graph
    :param project_file_path: The path of the project file to create
    :return: False if the connection is not found, else True
    """
    # Get connection information
    uri = get_postgis_connection_uri_from_name(connection_name)
    if not uri:
        return False
    connection_info = uri.connectionInfo()

    # Read in the template file
    template_file = plugin_path("resources", "qgis", "plugin_admin.qgs")
    with open(template_file, "r") as fin:
        filedata = fin.read()

    # Replace the database connection information, the SRID of the data sources,
    # and the QGIS project variable
    template_crs = QgsCoordinateReferenceSystem(f"EPSG:{srid_value()}")
    filedata = filedata.replace("service='pg_road_network_service'", connection_info)
    filedata = re.sub(rf"\bsrid={srid_value()}\b", f"srid={crs.postgisSrid()}", filedata)
    filedata = filedata.replace("PLUGIN road_network", connection_name)

    # Systems and extents of the project and of its layers
    document = QDomDocument()
    document.setContent(filedata)
    map_extent = update_project_crs_and_extents(document, connection_name, template_crs, crs)
    with open(project_file_path, "w") as fout:
        fout.write(document.toString(2))

    # Copy the Lizmap configuration, with the same system and extent as the project
    config_file = Path(plugin_path("resources", "qgis", "plugin_admin.qgs.cfg"))
    if config_file.exists():
        target_config_file = Path(f"{project_file_path}.cfg")
        shutil.copyfile(config_file, target_config_file)
        update_lizmap_config(target_config_file, template_crs, crs, map_extent)

    return True
