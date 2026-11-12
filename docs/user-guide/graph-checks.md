# Check and fix the road graph

After an [import](import.md), the road graph often contains issues coming from the source data:
crossing edges which are not cut, edges which should touch but do not, edges drawn in the opposite
direction of the road, branches of a road using the code of the main road, etc.

The plugin provides tools to **find these issues** and, when possible, to **fix them automatically**.

## Workflow

The issues must be fixed in an **editing session**, like any other modification of the road graph
(see [Road graph editing](graph-editing.md)). Even after an import, use an editing session:
draw a large editing session polygon if needed.

1. Create an editing session covering the area to check.
2. Run the algorithm **Check the road graph** on the schema of the editing session.
   It creates two layers: a **point** layer locating each issue,
   and a **line** layer showing the issues for which a line is useful.
3. Review the issues, and fix them:
    * one by one with a **right click** on the issue (see [Fix an issue with a right click](#fix-an-issue-with-a-right-click)),
    * or in batch: select the issues to fix automatically, then run the algorithm **Fix the road graph issues**
      with the option **Selected features only**. The issues already fixed are skipped,
    * or manually, for the issues without fix action (see the description of each check).
4. Run the algorithm **Check the road graph** again to check the result.
5. Merge the editing session.

If the layers of the editing session are in **editing mode**, the fixes are run in their **transaction**:
save the modifications of the layers afterwards, or cancel them. Else the fixes are saved at once.

## Algorithms

The algorithm **Check the road graph** runs the checks on the schema of the editing session (or of the production,
only to get an overview of the issues), for all the roads or only some of them, and creates the issues layers.
The advanced parameter **Options of the checks** overrides the default options of the checks.

![Algorithm Check the road graph](../media/user-guide/processing_check_graph.png)

The algorithm **Fix the road graph issues** fixes the issues of one of these layers, in the order of their check codes,
with their fix action. Check the option **Selected features only** to fix only the selected issues.
The issues without fix action, and the issues already fixed, are skipped.

![Algorithm Fix the road graph issues](../media/user-guide/processing_fix_graph_issues.png)

## Fix an issue with a right click

The layers created by the algorithm Check the road graph have three **actions**.
Use the `Identify Features` tool and **right-click** on an issue in the map,
or use the `Run Feature Action` tool of the `Attributes` toolbar:

* **Fix this issue**: fix the issue with its fix action. Nothing is done if the issue has no fix action,
* **Choose the fix of this issue…**: choose the fix action among the actions of the check, then fix the issue.
  For example, for the check G01, cut the edges or mark them as not connected (bridge),
  and in this case choose the edge to mark,
* **Select the edges of this issue**: select the edges of the issue in the edges layer of the editing session
  (`road_graph_editing.edges`) and zoom on them, for example to fix the issue manually.

In this example of the test data, the roads of an interchange cross the N814 on bridges without being connected,
and an access road arrives on the roundabout without splitting it. This last issue has already been fixed:
it is displayed in grey. A right click on a crossing of the N814 shows the actions:

![Right click on an issue](../media/user-guide/graph_checks_right_click.jpg)

The action **Choose the fix of this issue…** lists the fix actions of the check:

![Choose the fix action](../media/user-guide/graph_checks_choose_fix.png)

For the action `mark_as_bridge`, choose then the edge which must not be connected, or all the edges:

![Choose the edge to mark](../media/user-guide/graph_checks_choose_edge.png)

The result of the fix is stored in the fields `fix_status` (`Fixed`, `Already fixed` or `Not fixed`)
and `fix_message` of the points and lines layers of the issues, and the fixed issues are displayed in grey.
The edges layers of the editing session are refreshed.

The titles of the checks and of the actions, the labels of the fix actions and statuses and the messages
are translated in the language of QGIS. The detailed messages returned by the database are in English.

## Checks

Each issue has:

* a **check code** and title, and a **severity**: `error` or `warning`,
* the **road code**, and the ids of the **edges**, **nodes** and **markers** concerned,
* some **details**, for example a distance or an angle,
* the **fix action** which can fix it automatically, and its parameters,
  or nothing if the issue must be fixed manually.

The checks are numbered in the recommended order of the fixes:
the algorithm Fix the road graph issues fixes the issues in this order.

| Code | Issue | Fix |
|------|-------|-----|
| [G01](#g01-crossing-edges-without-node) | Edges crossing or touching each other without node | `cut_edges` or `mark_as_bridge` |
| [G02](#g02-edge-ending-near-another-edge) | Edge ending near another edge without being connected (T junction) | `extend_edge` |
| [G03](#g03-nodes-close-to-each-other) | Nodes close to each other | `merge_nodes` |

To fix an issue with another action than the proposed one, or to choose the action of an issue without action,
toggle the editing mode of the point layer created by the algorithm Check the road graph,
and change the field `fix_action` (and `fix_params` if needed) before running the algorithm Fix the road graph issues.

### G01: Crossing edges without node

Two edges crossing or touching each other must be split by a node, unless they are not connected,
for example on a **bridge** or in a **tunnel**: the edges with the field `no_intersection_cutting` set to `True`
are ignored (see [Edges which must not be cut](graph-editing.md#edges-which-must-not-be-cut)).

The check returns one issue for each point where two edges cross or touch each other,
if this point is not the start or end point of both edges (50 cm tolerance, like the triggers splitting the edges).
The field `details` gives:

* `crossing_type`:
    * `touching`: the point is the start or end point of one of the edges, for example a road arriving on another road
      without splitting it. The edges are surely connected: the fix action is `cut_edges`,
    * `crossing`: the edges cross each other. They may be on a bridge: **the fix action must be chosen for each issue**,
* `road_codes` and `road_classes`: the codes and classes of the roads of the two edges,
  for example a highway crossing a departmental road is probably a bridge,
* `angle`: the angle between the two edges, from 0 (parallel) to 90 degrees (perpendicular).

Fix actions:

* `cut_edges`: create a node at the point, or use the existing node less than 50 cm from it,
  which splits the edges passing there,
* `mark_as_bridge`: set the field `no_intersection_cutting` to `True` for the two edges,
  or only for the edge given by the key `edge_id` added to `fix_params`, for example
  `{"point": "SRID=2154;POINT(455144.6 6905485.8)", "edge_ids": [2839, 2841], "edge_id": 2839}`.

If most of the crossings are bridges, or are crossroads, set the option `crossing_fix_action`
of the check to `mark_as_bridge` or `cut_edges`: the issues of type `crossing` then get this fix action.
Check the result carefully afterwards.

```sql
UPDATE road_graph.glossary_graph_check
SET default_options = default_options || '{"crossing_fix_action": "mark_as_bridge"}'
WHERE code = 'G01';
```

### G02: Edge ending near another edge

A road arriving on another road must be connected to it. In the source data, it often stops
a few decimeters before the other road, or a little beyond it.

The check returns one issue for each **free end** of an edge, i.e. an end with no other edge connected to it,
located less than the **tolerance** from another edge: option `tolerance`, **2 m** by default.
Are ignored:

* the edges connected to the opposite end of the edge, for example its next edge if it is very short,
* the edges crossed by the edge near its free end: the edge goes a little beyond the other edge.
  The crossing is found by the check [G01](#g01-crossing-edges-without-node).

When two free ends are close to each other, only one issue is returned.

The **point to join** is the closest point of the other edge, or its start or end point
if it is less than 50 cm from it. The field `details` gives the road codes of the two edges,
the `distance`, and the `join_type`: `edge` (middle of the other edge) or `edge_end`.

Fix action:

* `extend_edge`: add the point to join at the free end of the edge. The triggers move the node of the free end
  to this point, and split the other edge there. Like any edge geometry, the coordinates are rounded to 10 cm.

```sql
-- Find the gaps up to 3 m
UPDATE road_graph.glossary_graph_check
SET default_options = default_options || '{"tolerance": 3}'
WHERE code = 'G02';
```

### G03: Nodes close to each other

Two nodes a few centimeters or decimeters from each other should usually be the same node:
the roads arriving there are not connected in the graph.

The check returns one issue for each pair of nodes less than the **tolerance** from each other:
option `tolerance`, **1 m** by default. The nodes linked by an edge are ignored: this edge is very short,
and must be examined with the check of the short edges.

The **node to keep** is the node with the **lowest id**, like in the triggers, which use the node with the lowest id
less than 50 cm from the start or end point of an edge. It is the first node of `node_ids`, and the point of the issue.
The field `details` gives the road codes of the edges of the two nodes and the `distance`.

Many of these issues are also found by the check [G02](#g02-edge-ending-near-another-edge),
when an edge ends near the start or end point of another edge: they are skipped once fixed by G02.

Fix action:

* `merge_nodes`: move the start and end points of the edges of the other node to the node to keep,
  then delete the other node.

## Configuration of the checks

The checks are listed in the table `road_graph.glossary_graph_check`, with for each check:
its code, title, severity, function, fix actions and description.
The rows are added by the plugin migrations. The administrator can modify two columns:

* `default_options`: the default options of the check, for example its tolerance in meters,
* `is_active`: uncheck it to stop running the check by default. It is still run if it is explicitly asked.

```sql
-- Give the action mark_as_bridge to the edges crossing each other found by the check G01
UPDATE road_graph.glossary_graph_check
SET default_options = default_options || '{"crossing_fix_action": "mark_as_bridge"}'
WHERE code = 'G01';
```

The checks are always read from the production schema, even when the editing session is checked.

## Database functions

The algorithms use the following database functions, which can also be used directly in SQL.
They are installed in the production schema `road_graph` and in the editing schema `road_graph_editing`.

* `road_graph.check_graph(_checks, _road_codes, _options)`: run the checks and return one line per issue.
  All the active checks are run if `_checks` is `NULL`, and all the roads are checked if `_road_codes` is `NULL`.
  The parameter `_options` overrides the default options of the checks, with the check code as key.

```sql
-- Check all the roads of the editing session
SELECT * FROM road_graph_editing.check_graph();

-- Check only some roads, with some checks and an option
SELECT *
FROM road_graph_editing.check_graph(
    ARRAY['G01'],
    ARRAY['D40', 'D16'],
    '{"G01": {"crossing_fix_action": "cut_edges"}}'
);
```

* `road_graph_editing.fix_graph_issue(_fix_action, _params)`: fix an issue with its fix action and parameters.
  It only works in the editing schema. It returns a JSON object with the keys `status`
  (`success`, `skipped` if the issue has already been fixed, or `error`) and `message`.

```sql
SELECT road_graph_editing.fix_graph_issue(i.fix_action, i.fix_params)
FROM road_graph_editing.check_graph(ARRAY['G01']) AS i
WHERE i.fix_action IS NOT NULL
ORDER BY i.id;
```
