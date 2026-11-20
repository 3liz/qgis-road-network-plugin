"""Tools shared by the road graph checks algorithms and the QGIS actions on the issues layers.

The issues of the editing session are stored in the table road_graph_editing.graph_issues by the algorithm
Check the road graph, and displayed by two layers of the project (points and lines), added by the algorithm.
The issues of the production schema are only checked to get an overview,
in temporary layers.

The issues are fixed with the algorithm Fix the road graph issues,
or one by one with the actions added to the layers (right click on an issue).
"""

import json

from typing import Optional

from qgis.core import (
    Qgis,
    QgsAction,
    QgsCoordinateReferenceSystem,
    QgsDataSourceUri,
    QgsEditorWidgetSetup,
    QgsFeature,
    QgsFeatureRequest,
    QgsLineSymbol,
    QgsMarkerSymbol,
    QgsPalLayerSettings,
    QgsProcessingLayerPostProcessorInterface,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsRuleBasedRenderer,
    QgsTransaction,
    QgsVectorLayer,
    QgsVectorLayerSimpleLabeling,
    QgsWkbTypes,
)

from .plugin_tools.i18n import tr

# The issues can only be fixed in the editing session
EDITING_SCHEMA = "road_graph_editing"
# Table of the issues of the editing session, and its geometry columns: point, and lines showing the issue
ISSUES_TABLE = "graph_issues"
ISSUES_POINTS_COLUMN = "geom"
ISSUES_LINES_COLUMN = "geom_detail"

# Custom properties of the issues layers
# Unique id of the run of the algorithm Check the road graph, shared by its points and lines layers
ISSUES_RUN_PROPERTY = "roadnetwork/graph_issues_run"
# Name of the PostgreSQL connection used to check the road graph
CONNECTION_PROPERTY = "roadnetwork/connection_name"

# Prefix of the exceptions raised to get the status of a fix run in a layer transaction
TRANSACTION_STATUS_PREFIX = "roadnetwork_fix_status_"

# Actions added to the issues layers, run by the plugin (see roadnetwork/actions/graph_issues.py)
ISSUE_ACTIONS = ("fix_graph_issue", "choose_graph_issue_fix", "select_graph_issue_edges")


def check_titles() -> dict:
    """Translated titles of the checks, by check code.

    The titles stored in the database are in English: they are translated with this list,
    which also allows to extract them for the translation.

    :return: The translated title of each check code
    """
    return {
        "G01": tr("Crossing edges without node"),
        "G02": tr("Edge ending near another edge without being connected"),
        "G03": tr("Nodes close to each other"),
        "G04": tr("Useless node"),
        "G05": tr("Edges in the opposite direction of the road"),
        "G06": tr("Previous and next edges inconsistent with the geometry"),
        "G07": tr("Road with branches"),
        "G08": tr("Parallel edges of the same road"),
        "G09": tr("Short edge"),
        "G10": tr("First marker not at the start of the road"),
        "G11": tr("Markers not in the order of the road"),
    }


def fix_action_labels() -> dict:
    """Translated labels of the fix actions, by action name.

    :return: The translated label of each fix action
    """
    return {
        "cut_edges": tr("Cut the edges with a node"),
        "mark_as_bridge": tr("Mark the edges as not connected (bridge, tunnel)"),
        "extend_edge": tr("Extend the edge to the other edge"),
        "merge_nodes": tr("Merge the nodes"),
        "delete_node": tr("Delete the node and merge its edges"),
        "reverse_edges": tr("Reverse the edges"),
        "collapse_edge": tr("Remove the edge and merge its nodes"),
        "move_marker": tr("Move the marker to the start of the road"),
        "relink_edges": tr("Set the previous and next edges from the geometry"),
    }


def fix_status_labels() -> dict:
    """Translated labels of the fix statuses, by status.

    :return: The translated label of each status: success, skipped and error
    """
    return {
        "success": tr("Fixed"),
        "skipped": tr("Already fixed"),
        "error": tr("Not fixed"),
    }


def severity_labels() -> dict:
    """Translated labels of the severities of the issues.

    :return: The translated label of each severity: error and warning
    """
    return {
        "error": tr("Error"),
        "warning": tr("Warning"),
    }


def editing_schema_layers(project: QgsProject, table: Optional[str] = None) -> list:
    """Get the PostgreSQL layers of the editing schema in the project.

    :param project: The QGIS project
    :param table: The name of the table, e.g. edges, or None for all the tables
    :return: The list of the vector layers
    """
    layers = []
    for layer in project.mapLayers().values():
        if not isinstance(layer, QgsVectorLayer) or layer.providerType() != "postgres":
            continue
        uri = QgsDataSourceUri(layer.source())
        if uri.schema() != EDITING_SCHEMA:
            continue
        if table and uri.table() != table:
            continue
        layers.append(layer)
    return layers


def sql_literal(value: Optional[str]) -> str:
    """Quote a text value as a SQL literal.

    :param value: The text value, or None
    :return: The quoted literal, or NULL
    """
    if value is None:
        return "NULL"
    return "'{}'".format(value.replace("'", "''"))


def sql_text_array(values: Optional[list]) -> str:
    """Write a list of texts as a SQL text array.

    :param values: The texts, or None or an empty list for NULL
    :return: The SQL array, for example ARRAY['D40', 'D16']::text[], or NULL::text[]
    """
    if not values:
        return "NULL::text[]"
    return "ARRAY[{}]::text[]".format(", ".join(sql_literal(v) for v in values))


def id_list(value: object) -> list:
    """Get the ids of a field of an issue: a list for the table of the issues, a text for a temporary layer.

    :param value: The value of the field edge_ids, node_ids or marker_ids
    :return: The list of the ids
    """
    if isinstance(value, (list, tuple)):
        return [int(v) for v in value if v is not None]
    if isinstance(value, str):
        return [int(v) for v in value.split(",") if v.strip()]
    return []


def json_dict(value: object) -> dict:
    """Get a JSON field of an issue: a dict for the table of the issues, a text for a temporary layer.

    :param value: The value of the field details or fix_params
    :return: The JSON object, empty if the field is NULL
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        return json.loads(value)
    return {}


def issue_road_codes(feature: QgsFeature) -> list:
    """Codes of the roads of an issue: its road code, and the road codes of its details.

    :param feature: The feature of the issue
    :return: The sorted list of the road codes
    """
    road_codes = set(json_dict(feature["details"]).get("road_codes") or [])
    if feature["road_code"]:
        road_codes.add(feature["road_code"])
    return sorted(road_codes)


def is_issues_table_layer(layer: object) -> bool:
    """Check if a layer displays the table of the issues of the editing session.

    :param layer: The layer
    :return: True for a PostgreSQL layer of the table road_graph_editing.graph_issues
    """
    if not isinstance(layer, QgsVectorLayer) or layer.providerType() != "postgres":
        return False
    uri = QgsDataSourceUri(layer.source())
    return uri.schema() == EDITING_SCHEMA and uri.table() == ISSUES_TABLE


def editing_transaction(project: QgsProject) -> tuple:
    """Get the transaction of the layers of the editing schema in editing mode, if any.

    :param project: The QGIS project
    :return: The transaction, or None, and the layers of the editing schema in editing mode
    """
    editing_layers = [layer for layer in editing_schema_layers(project) if layer.isEditable()]
    transaction = next(
        (
            layer.dataProvider().transaction()
            for layer in editing_layers
            if layer.dataProvider().transaction() is not None
        ),
        None,
    )
    return transaction, editing_layers


def reload_editing_layers(project: QgsProject, transaction: Optional[QgsTransaction]):
    """Refresh the layers of the editing schema after a modification of the database.

    :param project: The QGIS project
    :param transaction: The transaction in which the modification has been run, or None if it is committed
    """
    for layer in editing_schema_layers(project):
        # In a transaction, the layers read the modifications through the same connection
        if transaction is None or is_issues_table_layer(layer):
            layer.reload()
        layer.triggerRepaint()


def run_fix_sql(connection_name: str, fix_call: str, project: QgsProject) -> tuple:
    """Run a SQL function fixing an issue of the road graph in the editing session.

    If the layers of the editing schema are in editing mode with a transaction, the fix is run in it:
    the user can then save or cancel it. Else it is run and committed with the given connection,
    unless some layers in editing mode without transaction have modifications which are not saved yet.

    :param connection_name: The name of the PostgreSQL connection
    :param fix_call: The SQL call of the function, returning a JSON object with the keys status and message
    :param project: The QGIS project, to find the layers of the editing schema
    :return: The status (success, skipped or error) and the message
    """
    # Layers of the editing schema in editing mode: run the fix in their transaction, if one of them has one
    transaction, editing_layers = editing_transaction(project)
    if transaction is None:
        # Layers in editing mode without transaction: their modifications are kept by QGIS until saved.
        # The fix cannot be run beside them: saving them afterwards could overwrite it, or fail.
        # Without modification, the fix is run with the connection below, and the layers are reloaded
        modified_layers = [layer.name() for layer in editing_layers if layer.isModified()]
        if modified_layers:
            return (
                "error",
                tr(
                    "The layers {} of the editing session have modifications which are not saved: "
                    "save or cancel them before fixing the issues."
                ).format(", ".join(modified_layers)),
            )
    else:
        # The status is raised as an exception when it is not success:
        # the transaction is rolled back to its savepoint, and the message gives the status
        sql = f"""
            DO $fix$
            DECLARE
                result jsonb;
            BEGIN
                result = {fix_call};
                IF result->>'status' != 'success' THEN
                    RAISE EXCEPTION '{TRANSACTION_STATUS_PREFIX}%: %', result->>'status', result->>'message';
                END IF;
            END;
            $fix$
        """
        ok, error = transaction.executeSql(sql, True, "")
        reload_editing_layers(project, transaction)
        if ok:
            return "success", tr(
                "The issue has been fixed: save the modifications of the editing session layers."
            )
        if TRANSACTION_STATUS_PREFIX in error:
            status_message = error.split(TRANSACTION_STATUS_PREFIX, 1)[1]
            status, _, message = status_message.partition(":")
            return status.strip(), message.strip()
        return "error", error

    # Else run the fix with the connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.findConnection(connection_name) if connection_name else None
    if not connection:
        return "error", tr("The PostgreSQL connection {} has not been found").format(connection_name)
    try:
        data = connection.executeSql(f"SELECT ({fix_call})::text")
        result = json.loads(data[0][0])
    except (QgsProviderConnectionException, ValueError, IndexError) as e:
        return "error", str(e)

    reload_editing_layers(project, None)
    return result.get("status", "error"), result.get("message", "")


def run_fix(connection_name: str, fix_action: str, fix_params: str, project: QgsProject) -> tuple:
    """Fix an issue of the road graph in the editing session, with a fix action and its parameters.

    See run_fix_sql for the transaction used.

    :param connection_name: The name of the PostgreSQL connection
    :param fix_action: The fix action, e.g. cut_edges
    :param fix_params: The parameters of the fix action, as a JSON string
    :param project: The QGIS project, to find the layers of the editing schema
    :return: The status (success, skipped or error) and the message
    """
    fix_call = "{}.fix_graph_issue({}, {}::jsonb)".format(
        EDITING_SCHEMA, sql_literal(fix_action), sql_literal(fix_params or "{}")
    )
    return run_fix_sql(connection_name, fix_call, project)


def run_stored_fix(
    connection_name: str,
    issue_id: int,
    project: QgsProject,
    fix_action: Optional[str] = None,
    fix_params: Optional[str] = None,
    refresh: bool = True,
) -> tuple:
    """Fix an issue stored in the table of the issues of the editing session.

    The result is stored in the table. If refresh is True and the fix did not fail,
    the roads of the issue are checked again: the fixed issue is deleted, and the new issues are added.
    See run_fix_sql for the transaction used.

    :param connection_name: The name of the PostgreSQL connection
    :param issue_id: The id of the issue in the table
    :param project: The QGIS project, to find the layers of the editing schema
    :param fix_action: The fix action chosen by the user, or None for the fix action of the issue
    :param fix_params: The parameters of the chosen fix action, as a JSON string, or None
    :param refresh: True to check again the roads of the issue after the fix
    :return: The status (success, skipped or error) and the message
    """
    fix_call = "{}.fix_stored_graph_issue({}, {}, {}::jsonb, {})".format(
        EDITING_SCHEMA,
        int(issue_id),
        sql_literal(fix_action),
        sql_literal(fix_params),
        "True" if refresh else "False",
    )
    return run_fix_sql(connection_name, fix_call, project)


def refresh_issues(
    connection_name: str,
    project: QgsProject,
    checks: Optional[list] = None,
    road_codes: Optional[list] = None,
    options: Optional[str] = None,
) -> tuple:
    """Run the checks of the road graph and update the table of the issues of the editing session.

    If the layers of the editing schema are in editing mode with a transaction, the checks are run in it,
    to check the modifications which are not saved yet. Else they are run and committed with the connection.

    :param connection_name: The name of the PostgreSQL connection
    :param project: The QGIS project, to find the layers of the editing schema
    :param checks: The codes of the checks to run, or None for all the checks
    :param road_codes: The codes of the roads to check, or None for all the roads
    :param options: The options of the checks, as a JSON string, or None
    :return: True if the checks have been run, and the error message if not
    """
    sql = "SELECT {}.refresh_graph_issues({}, {}, {}::jsonb)".format(
        EDITING_SCHEMA, sql_text_array(checks), sql_text_array(road_codes), sql_literal(options or None)
    )
    transaction, _ = editing_transaction(project)
    if transaction is not None:
        ok, error = transaction.executeSql(sql, True, "")
        reload_editing_layers(project, transaction)
        return ok, error

    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.findConnection(connection_name) if connection_name else None
    if not connection:
        return False, tr("The PostgreSQL connection {} has not been found").format(connection_name)
    try:
        connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        return False, str(e)
    reload_editing_layers(project, None)
    return True, ""


def add_issues_layers(project: QgsProject, connection_name: str, crs: QgsCoordinateReferenceSystem) -> list:
    """Add the layers of the table of the issues of the editing session to the project, if they are missing.

    The layers are added in the group of the edges layer of the editing session, if any.
    The existing layers are set up again, for example to update their actions.

    :param project: The QGIS project
    :param connection_name: The name of the PostgreSQL connection
    :param crs: The coordinate reference system of the road graph
    :return: The points layer and the lines layer
    """
    layers = []
    for geometry_column, name in (
        (ISSUES_POINTS_COLUMN, tr("Road graph issues (points)")),
        (ISSUES_LINES_COLUMN, tr("Road graph issues (lines)")),
    ):
        layer = next(
            (
                layer
                for layer in editing_schema_layers(project, ISSUES_TABLE)
                if QgsDataSourceUri(layer.source()).geometryColumn() == geometry_column
            ),
            None,
        )
        if layer is None:
            metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
            connection = metadata.findConnection(connection_name)
            uri = QgsDataSourceUri(connection.uri())
            # The lines layer only displays the issues with lines
            sql_filter = f'"{geometry_column}" IS NOT NULL' if geometry_column == ISSUES_LINES_COLUMN else ""
            uri.setDataSource(EDITING_SCHEMA, ISSUES_TABLE, geometry_column, sql_filter, "id")
            uri.setSrid(str(crs.postgisSrid()))
            uri.setWkbType(
                QgsWkbTypes.Point if geometry_column == ISSUES_POINTS_COLUMN else QgsWkbTypes.MultiLineString
            )
            layer = QgsVectorLayer(uri.uri(False), name, "postgres")
            project.addMapLayer(layer, False)

            # In the group of the edges layer of the editing session, above it, else at the top
            edges_layers = editing_schema_layers(project, "edges")
            edges_node = project.layerTreeRoot().findLayer(edges_layers[0]) if edges_layers else None
            group = edges_node.parent() if edges_node else project.layerTreeRoot()
            group.insertLayer(0, layer)
        setup_issues_layer(layer, connection_name)
        layers.append(layer)
    return layers


def set_issue_fix_result(
    project: QgsProject,
    run_id: str,
    issue_id: int,
    status: str,
    message: str,
    fix_action: Optional[str] = None,
    fix_params: Optional[str] = None,
    layers: Optional[list] = None,
):
    """Store the result of a fix in the issues layers of the same run of the algorithm Check the road graph.

    The points and lines layers of the run are updated, if they are in the project,
    and the given layers, for example the input layer of the algorithm Fix the road graph issues.

    :param project: The QGIS project
    :param run_id: The id of the run, stored in the custom property of the issues layers
    :param issue_id: The id of the issue (field id)
    :param status: The status of the fix: success, skipped or error
    :param message: The message of the fix
    :param fix_action: The fix action, if it has been chosen by the user, else None
    :param fix_params: The parameters of the fix action, if they have been chosen by the user, else None
    :param layers: Other layers to update, if they are not in the project
    """
    # Layers of the run: the layers of the project, and the given layers
    run_layers = {}
    for layer in list(project.mapLayers().values()) + (layers or []):
        if isinstance(layer, QgsVectorLayer) and layer.customProperty(ISSUES_RUN_PROPERTY) == run_id:
            run_layers[layer.id()] = layer

    for layer in run_layers.values():
        # Values to store
        values = {"fix_status": status, "fix_message": message}
        if fix_action is not None:
            values["fix_action"] = fix_action
        if fix_params is not None:
            values["fix_params"] = fix_params
        attributes = {
            layer.fields().indexOf(name): value
            for name, value in values.items()
            if layer.fields().indexOf(name) >= 0
        }

        # Update the features of the issue, in the edit buffer if the layer is in editing mode
        request = QgsFeatureRequest().setFilterExpression(f'"id" = {int(issue_id)}')
        for feature in layer.getFeatures(request):
            if layer.isEditable():
                for index, value in attributes.items():
                    layer.changeAttributeValue(feature.id(), index, value)
            else:
                layer.dataProvider().changeAttributeValues({feature.id(): attributes})
        layer.triggerRepaint()


def setup_issues_layer(layer: QgsVectorLayer, connection_name: str, run_id: Optional[str] = None):
    """Set up a layer of the issues: custom properties, actions, fields aliases and widgets, style and labels.

    The layers of the table of the issues of the editing session are set up when they are added
    by the algorithm Check the road graph, and the temporary layers when they are created by this algorithm.

    :param layer: The points or lines layer of the issues
    :param connection_name: The name of the PostgreSQL connection used to check the road graph
    :param run_id: The unique id of the run, shared by the points and lines temporary layers,
        or None for the layers of the table of the issues
    """
    # Custom properties used by the actions
    if run_id:
        layer.setCustomProperty(ISSUES_RUN_PROPERTY, run_id)
    layer.setCustomProperty(CONNECTION_PROPERTY, connection_name)

    # Actions run by the plugin on a feature (right click with the Identify Features tool,
    # attribute table, or Run Feature Action tool)
    titles = {
        "fix_graph_issue": tr("Fix this issue"),
        "choose_graph_issue_fix": tr("Choose the fix of this issue…"),
        "select_graph_issue_edges": tr("Select the edges of this issue"),
    }
    actions = layer.actions()
    for action in actions.actions():
        if action.command().find("_graph_issue") >= 0:
            actions.removeAction(action.id())
    for name in ISSUE_ACTIONS:
        command = (
            "from qgis.utils import plugins\n"
            "plugins['roadnetwork'].run_action(\n"
            f"    '{name}',\n"
            "    layer_id='[% @layer_id %]',\n"
            '    issue_id=[% "id" %],\n'
            ")\n"
        )
        action = QgsAction(
            Qgis.AttributeActionType.GenericPython,
            titles[name],
            command,
            "",
            False,
            titles[name],
            {"Feature", "Canvas"},
        )
        actions.addAction(action)

    # Translated aliases of the fields
    aliases = {
        "id": tr("Id"),
        "schema_name": tr("Schema"),
        "check_code": tr("Check code"),
        "check_title": tr("Check"),
        "severity": tr("Severity"),
        "road_code": tr("Road code"),
        "edge_ids": tr("Edges"),
        "node_ids": tr("Nodes"),
        "marker_ids": tr("Markers"),
        "details": tr("Details"),
        "fix_action": tr("Fix action"),
        "fix_params": tr("Fix parameters"),
        "fix_status": tr("Fix status"),
        "fix_message": tr("Fix message"),
        "checked_at": tr("Checked at"),
    }
    for name, alias in aliases.items():
        index = layer.fields().indexOf(name)
        if index >= 0:
            layer.setFieldAlias(index, alias)

    # Translated labels of the values
    for name, labels in (
        ("severity", severity_labels()),
        ("fix_action", fix_action_labels()),
        ("fix_status", fix_status_labels()),
    ):
        index = layer.fields().indexOf(name)
        if index >= 0:
            value_map = [{label: value} for value, label in labels.items()]
            layer.setEditorWidgetSetup(index, QgsEditorWidgetSetup("ValueMap", {"map": value_map}))

    # Style: the fixed issues in grey, the other errors in red and the other warnings in orange
    is_line = layer.geometryType() == QgsWkbTypes.LineGeometry
    not_fixed = '("fix_status" IS NULL OR "fix_status" = \'error\')'
    root_rule = QgsRuleBasedRenderer.Rule(None)
    for label, expression, color in (
        (tr("Fixed"), "\"fix_status\" IN ('success', 'skipped')", "150,150,150"),
        (tr("Error"), f"\"severity\" = 'error' AND {not_fixed}", "227,26,28"),
        (tr("Warning"), f"\"severity\" = 'warning' AND {not_fixed}", "255,127,0"),
    ):
        if is_line:
            symbol = QgsLineSymbol.createSimple({"color": color, "width": "1.2", "line_style": "dash"})
        else:
            symbol = QgsMarkerSymbol.createSimple(
                {"name": "circle", "color": color, "size": "4", "outline_color": "255,255,255"}
            )
        root_rule.appendChild(QgsRuleBasedRenderer.Rule(symbol, 0, 0, expression, label))
    layer.setRenderer(QgsRuleBasedRenderer(root_rule))

    # Labels: the check code of the points
    if not is_line:
        settings = QgsPalLayerSettings()
        settings.fieldName = "check_code"
        settings.placement = Qgis.LabelPlacement.OverPoint
        settings.yOffset = -3
        layer.setLabeling(QgsVectorLayerSimpleLabeling(settings))
        layer.setLabelsEnabled(True)

    layer.triggerRepaint()


class IssuesLayerPostProcessor(QgsProcessingLayerPostProcessorInterface):
    """Set up the issues layers created by the algorithm Check the road graph, once loaded in the project."""

    def __init__(self, connection_name: str, run_id: str):
        """Create the post processor.

        :param connection_name: The name of the PostgreSQL connection used to check the road graph
        :param run_id: The unique id of the run, shared by the points and lines layers
        """
        super().__init__()
        self.connection_name = connection_name
        self.run_id = run_id

    def postProcessLayer(self, layer, context, feedback):
        """Set up the layer.

        :param layer: The loaded layer
        :param context: The processing context
        :param feedback: The processing feedback
        """
        if isinstance(layer, QgsVectorLayer):
            setup_issues_layer(layer, self.connection_name, self.run_id)


# The post processors must be kept alive until the layers are loaded
POST_PROCESSORS: list[IssuesLayerPostProcessor] = []
