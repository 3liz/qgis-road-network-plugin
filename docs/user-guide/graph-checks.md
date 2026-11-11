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
3. Review the issues. Fix manually the issues without a fix action,
   or the issues which must not be fixed automatically (see the description of each check).
4. Select the issues to fix automatically, then run the algorithm **Fix the road graph issues**
   with the option **Selected features only**.
5. Run the algorithm **Check the road graph** again to check the result.
6. Merge the editing session.

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

## Configuration of the checks

The checks are listed in the table `road_graph.glossary_graph_check`, with for each check:
its code, title, severity, function, fix actions and description.
The rows are added by the plugin migrations. The administrator can modify two columns:

* `default_options`: the default options of the check, for example its tolerance in meters,
* `is_active`: uncheck it to stop running the check by default. It is still run if it is explicitly asked.

```sql
-- Use a tolerance of 3 m for the check G02
UPDATE road_graph.glossary_graph_check
SET default_options = default_options || '{"tolerance": 3}'
WHERE code = 'G02';
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

-- Check only some roads, with some checks and a specific tolerance
SELECT *
FROM road_graph_editing.check_graph(
    ARRAY['G01', 'G02'],
    ARRAY['D40', 'D16'],
    '{"G02": {"tolerance": 2}}'
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
