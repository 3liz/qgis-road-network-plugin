"""Tools shared by the road graph checks algorithms and the QGIS actions on the issues layers.

The issues layers are created by the algorithm Check the road graph.
Their features can be fixed with the algorithm Fix the road graph issues,
or one by one with the actions added to the layers (right click on an issue).
"""

import json

from typing import Optional

from qgis.core import (
    Qgis,
    QgsAction,
    QgsDataSourceUri,
    QgsEditorWidgetSetup,
    QgsFeatureRequest,
    QgsLineSymbol,
    QgsMarkerSymbol,
    QgsPalLayerSettings,
    QgsProcessingLayerPostProcessorInterface,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsRuleBasedRenderer,
    QgsVectorLayer,
    QgsVectorLayerSimpleLabeling,
    QgsWkbTypes,
)

from .plugin_tools.i18n import tr

# The issues can only be fixed in the editing session
EDITING_SCHEMA = "road_graph_editing"

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
        "G06": tr("Road with branches"),
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


def run_fix(connection_name: str, fix_action: str, fix_params: str, project: QgsProject) -> tuple:
    """Fix an issue of the road graph in the editing session.

    If the layers of the editing schema are in editing mode, the fix is run in their transaction:
    the user can then save or cancel it. Else it is run and committed with the given connection.

    :param connection_name: The name of the PostgreSQL connection
    :param fix_action: The fix action, e.g. cut_edges
    :param fix_params: The parameters of the fix action, as a JSON string
    :param project: The QGIS project, to find the layers of the editing schema
    :return: The status (success, skipped or error) and the message
    """
    # The fix action and its parameters are sent as SQL literals
    action_literal = "'{}'".format(fix_action.replace("'", "''"))
    params_literal = "'{}'::jsonb".format((fix_params or "{}").replace("'", "''"))

    # Layers of the editing schema in editing mode: run the fix in their transaction
    editing_layers = [layer for layer in editing_schema_layers(project) if layer.isEditable()]
    if editing_layers:
        transaction = editing_layers[0].dataProvider().transaction()
        if transaction is None:
            return (
                "error",
                tr(
                    "The layers of the editing session are in editing mode without transaction: "
                    "save the modifications and stop the editing mode before fixing the issues."
                ),
            )
        # The status is raised as an exception when it is not success:
        # the transaction is rolled back to its savepoint, and the message gives the status
        sql = f"""
            DO $fix$
            DECLARE
                result jsonb;
            BEGIN
                result = {EDITING_SCHEMA}.fix_graph_issue({action_literal}, {params_literal});
                IF result->>'status' != 'success' THEN
                    RAISE EXCEPTION '{TRANSACTION_STATUS_PREFIX}%: %', result->>'status', result->>'message';
                END IF;
            END;
            $fix$
        """
        ok, error = transaction.executeSql(sql, True, "")
        if ok:
            for layer in editing_layers:
                layer.triggerRepaint()
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
        data = connection.executeSql(
            f"SELECT {EDITING_SCHEMA}.fix_graph_issue({action_literal}, {params_literal})::text"
        )
        result = json.loads(data[0][0])
    except (QgsProviderConnectionException, ValueError, IndexError) as e:
        return "error", str(e)

    # Refresh the layers of the editing schema
    for layer in editing_schema_layers(project):
        layer.reload()
        layer.triggerRepaint()

    return result.get("status", "error"), result.get("message", "")


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


def setup_issues_layer(layer: QgsVectorLayer, connection_name: str, run_id: str):
    """Set up a layer created by the algorithm Check the road graph:
    custom properties, actions, fields aliases and widgets, style and labels.

    :param layer: The points or lines layer of the issues
    :param connection_name: The name of the PostgreSQL connection used to check the road graph
    :param run_id: The unique id of the run, shared by the points and lines layers
    """
    # Custom properties used by the actions
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
POST_PROCESSORS = []
