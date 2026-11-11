"""Actions of the layers of the road graph issues, created by the algorithm Check the road graph.

These lines are added to the layers by the algorithm, for example:

    from qgis.utils import plugins
    plugins['roadnetwork'].run_action(
        'fix_graph_issue',
        layer_id='[% @layer_id %]',
        issue_id=[% "id" %],
    )
"""

import json

from typing import Optional

from qgis.core import (
    Qgis,
    QgsFeature,
    QgsFeatureRequest,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsVectorLayer,
)
from qgis.PyQt.QtWidgets import QInputDialog
from qgis.utils import iface

from ..graph_issues import (
    CONNECTION_PROPERTY,
    EDITING_SCHEMA,
    ISSUES_RUN_PROPERTY,
    editing_schema_layers,
    fix_action_labels,
    fix_status_labels,
    run_fix,
    set_issue_fix_result,
)
from ..plugin_tools.i18n import tr
from ..processing.tools import get_connection_name
from .tools import display_error_message, display_message


def get_issue(layer_id: str, issue_id: int, action_name: str) -> Optional[tuple]:
    """Get the layer and the feature of an issue of the road graph.

    :param layer_id: The id of the issues layer
    :param issue_id: The id of the issue (field id)
    :param action_name: The name of the action, used in the error messages
    :return: The layer and the feature, or None if they are not found
    """
    layer = QgsProject.instance().mapLayer(layer_id)
    if not isinstance(layer, QgsVectorLayer) or not layer.customProperty(ISSUES_RUN_PROPERTY):
        display_error_message(
            action_name,
            tr("The layer {} is not a layer created by the algorithm Check the road graph").format(layer_id),
        )
        return None

    request = QgsFeatureRequest().setFilterExpression(f'"id" = {int(issue_id)}')
    feature = next(layer.getFeatures(request), None)
    if feature is None:
        display_error_message(action_name, tr("The issue {} has not been found").format(issue_id))
        return None

    if feature["schema_name"] != EDITING_SCHEMA:
        display_message(
            action_name,
            tr(
                "The issues must be fixed in the editing session: "
                "run the algorithm Check the road graph on the schema {}."
            ).format(EDITING_SCHEMA),
            Qgis.MessageLevel.Warning,
        )
        return None

    return layer, feature


def connection_name(layer: QgsVectorLayer) -> str:
    """Get the name of the PostgreSQL connection used to check the road graph.

    :param layer: The issues layer
    :return: The name of the connection stored in the layer, or else the one of the project
    """
    return layer.customProperty(CONNECTION_PROPERTY) or get_connection_name(QgsProject.instance())


def fix_and_report(
    layer: QgsVectorLayer,
    feature: QgsFeature,
    fix_action: str,
    fix_params: str,
    store_action: bool = False,
):
    """Fix an issue, store the result in the issues layers and display it.

    :param layer: The issues layer
    :param feature: The feature of the issue
    :param fix_action: The fix action
    :param fix_params: The parameters of the fix action, as a JSON string
    :param store_action: True to store the fix action and its parameters in the issue, if chosen by the user
    """
    project = QgsProject.instance()
    status, message = run_fix(connection_name(layer), fix_action, fix_params, project)

    set_issue_fix_result(
        project,
        layer.customProperty(ISSUES_RUN_PROPERTY),
        feature["id"],
        status,
        message,
        fix_action if store_action else None,
        fix_params if store_action else None,
    )

    title = tr("Issue {} - {}").format(feature["id"], fix_status_labels().get(status, status))
    if status == "success":
        display_message(title, message, Qgis.MessageLevel.Success)
    elif status == "skipped":
        display_message(title, message, Qgis.MessageLevel.Info)
    else:
        display_error_message(title, message)


def fix_graph_issue(layer_id: str, issue_id: int):
    """Fix an issue of the road graph with its fix action.

    :param layer_id: The id of the issues layer
    :param issue_id: The id of the issue (field id)
    """
    action_name = tr("Fix this issue")
    issue = get_issue(layer_id, issue_id, action_name)
    if issue is None:
        return
    layer, feature = issue

    if not feature["fix_action"]:
        display_message(
            action_name,
            tr(
                "This issue has no fix action: "
                "use the action Choose the fix of this issue, or fix it manually."
            ),
            Qgis.MessageLevel.Warning,
        )
        return

    fix_and_report(layer, feature, feature["fix_action"], feature["fix_params"])


def choose_graph_issue_fix(layer_id: str, issue_id: int):
    """Choose the fix action of an issue of the road graph among the actions of its check, then fix it.

    :param layer_id: The id of the issues layer
    :param issue_id: The id of the issue (field id)
    """
    action_name = tr("Choose the fix of this issue")
    issue = get_issue(layer_id, issue_id, action_name)
    if issue is None:
        return
    layer, feature = issue

    # Fix actions of the check
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection = metadata.findConnection(connection_name(layer) or "")
    if not connection:
        display_error_message(action_name, tr("The PostgreSQL connection has not been found"))
        return
    code = feature["check_code"].replace("'", "''")
    try:
        data = connection.executeSql(
            "SELECT array_to_string(fix_actions, ',') "
            f"FROM \"road_graph\".glossary_graph_check WHERE code = '{code}'"
        )
    except QgsProviderConnectionException as e:
        display_error_message(action_name, str(e))
        return
    fix_actions = [a for a in (data[0][0] if data and data[0][0] else "").split(",") if a]
    if not fix_actions:
        display_message(
            action_name,
            tr("This issue must be fixed manually."),
            Qgis.MessageLevel.Info,
        )
        return

    # Choose the action
    labels = fix_action_labels()
    items = [labels.get(a, a) for a in fix_actions]
    current = fix_actions.index(feature["fix_action"]) if feature["fix_action"] in fix_actions else 0
    item, ok = QInputDialog.getItem(
        iface.mainWindow(),
        action_name,
        tr("Issue {} - {}").format(feature["id"], feature["check_title"]),
        items,
        current,
        False,
    )
    if not ok:
        return
    fix_action = fix_actions[items.index(item)]

    # Choose the edge to mark as not connected: one of the edges, or all of them
    params = json.loads(feature["fix_params"] or "{}")
    edge_ids = params.get("edge_ids") or []
    if fix_action == "mark_as_bridge" and len(edge_ids) > 1:
        road_codes = json.loads(feature["details"] or "{}").get("road_codes") or []
        edge_items = [tr("All the edges")] + [
            tr("Edge {} - road {}").format(edge_id, road_codes[i] if i < len(road_codes) else "")
            for i, edge_id in enumerate(edge_ids)
        ]
        edge_item, ok = QInputDialog.getItem(
            iface.mainWindow(),
            action_name,
            tr("Edges to mark as not connected"),
            edge_items,
            0,
            False,
        )
        if not ok:
            return
        params.pop("edge_id", None)
        if edge_items.index(edge_item) > 0:
            params["edge_id"] = edge_ids[edge_items.index(edge_item) - 1]

    fix_and_report(layer, feature, fix_action, json.dumps(params), store_action=True)


def select_graph_issue_edges(layer_id: str, issue_id: int):
    """Select the edges of an issue in the edges layer of the editing session, and zoom on them.

    :param layer_id: The id of the issues layer
    :param issue_id: The id of the issue (field id)
    """
    action_name = tr("Select the edges of this issue")
    issue = get_issue(layer_id, issue_id, action_name)
    if issue is None:
        return
    _, feature = issue

    edge_ids = [int(i) for i in (feature["edge_ids"] or "").split(",") if i.strip()]
    if not edge_ids:
        display_message(action_name, tr("This issue has no edge."), Qgis.MessageLevel.Info)
        return

    edges_layers = editing_schema_layers(QgsProject.instance(), "edges")
    if not edges_layers:
        display_message(
            action_name,
            tr("The edges layer of the editing session ({}.edges) is not in the project.").format(
                EDITING_SCHEMA
            ),
            Qgis.MessageLevel.Warning,
        )
        return

    for edges_layer in edges_layers:
        edges_layer.selectByExpression('"id" IN ({})'.format(", ".join(str(i) for i in edge_ids)))
    iface.mapCanvas().zoomToSelected(edges_layers[0])
