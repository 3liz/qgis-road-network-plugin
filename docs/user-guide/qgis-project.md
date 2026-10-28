# QGIS project and tools

## Administration project

The algorithm `Create database local interface` creates a **QGIS administration project**
with all the layers, styles, labels and actions needed to visualize and edit the road network.

![QGIS administration project](../media/user-guide/qgis_administration_project.png)

The project contains the following groups of layers:

* `editing_sessions`: the areas of the editing sessions (created, cloned, edited),
  see [Road graph editing](graph-editing.md),
* `Editing`: the layers of the editing session schema `road_graph_editing`,
* `Data`: the production data
    * `managed_objects`: the list of business tables based on the road network,
      see [Managed objects](managed-objects.md),
    * `glossary_road_class` and `roads`,
    * `markers`: reference points (PR) and virtual markers (PRV),
    * `Topology`: the `nodes` and `edges`, styled by road class,
* `baselayers`: the background maps.

## Toolbar, menu and panels

### Toolbar

The plugin adds the toolbar **Road Network** to QGIS:

| Button | Action |
|--------|--------|
| `Get references under the cursor` | Activates the map tool showing the references of the clicked point, see [below](#get-references-on-map-click). Shortcut: `CTRL+SHIFT+K`. |
| `Clone data to the editing sandbox` | Runs the algorithm `Clone data to a new editing session`, see [Road graph editing](graph-editing.md#editing-rules) |
| `Merge editing sandbox data` | Runs the algorithm `Merge editing session data`, see [Road graph editing](graph-editing.md#editing-rules) |
| `Show/hide the tools dock` | Shows or hides the panel **RoadNetwork tools** |
| `Show/hide the administration dock` | Shows or hides the panel **RoadNetwork** (administration) |

### Menu

The menu `Plugins / RoadNetwork` contains the actions `Show/hide the tools dock`,
`Show/hide the administration dock`, and `Open online help`, which opens this documentation.

### Administration panel

The panel **RoadNetwork** is used by the administrator to install and configure the database:

* the group `Information` shows the **PostgreSQL connection** used by the project,
  the **versions** of the plugin and of the database structure, and checks that they match.
  If the database structure is older than the plugin, it asks to run the algorithm
  [Upgrade database structure](../installation/index.md#upgrade-database-structure),
* the group `Install and configure` runs the algorithms `Configure plugin`, `Create database structure`,
  `Create database local interface` and `Upgrade database structure`,
* the group `Import` runs the algorithm [Import data from template](import.md#run-the-import).

![RoadNetwork administration panel](../media/user-guide/qgis_administration_panel.png)

See [Processing algorithms](processing.md) for the description of these algorithms.

### Tools panel

The panel **RoadNetwork tools** shows the **references** of a point, and allows to find a point from references:

* the group `Localisation` concerns the production data (schema `road_graph`),
* the group `Localisation (editing)` concerns the data of the current editing session (schema `road_graph_editing`):
  during an editing session, the references can be different from the production ones,
* the group `Options` contains the checkbox `Get references on hover ?`, which updates the references
  when the mouse moves over the map, see [below](#get-references-on-map-click).

Each `Localisation` group contains the fields `Road`, `Marker`, `Abscissa`, `Cumulative`, `Offset` and `Side`
(`left` or `right`), filled with the references of the point clicked or hovered on the map.

To **find a point from references**, type the road, the marker, the abscissa and, optionally,
the offset and the side, then click on `Find` or press `Enter`:
the map is centered on the corresponding point, which flashes.

## Quick search

The QGIS **locator search bar** (`CTRL+K`) can be used to search a road or a position on a road.
No prefix is needed: the RoadNetwork results are displayed in the `RoadNetwork` group.

Type the road code and, optionally, the marker code, the abscissa, the offset and the side, separated by spaces:

* `D40`: highlights the whole road,
* `D40 1 151`: centers the map on the point `D40 PR 1 + 151m`,
* `D40 1 151 3 left`: same point, with an offset of `3m` on the left side.

![Quick search](../media/user-guide/qgis_search_ctrl_k.gif)

## Get references on map click

The tool `Get references under the cursor` shows the **references** of the point under the mouse:
road code, marker, abscissa, offset and side. It can be toggled with the plugin toolbar button
or with the shortcut `CTRL+SHIFT+K`. Hovering over the map is supported.

![Get references on map click](../media/user-guide/qgis_get_references_map_click.gif)


## Layer actions

Some layers of the project have **actions**, run by the RoadNetwork plugin,
which must be installed and activated:

| Layer              | Action                                                                  | Usage                                          |
|--------------------|-------------------------------------------------------------------------|------------------------------------------------|
| `editing_sessions` | [Create editing session](graph-editing.md#create-editing-session)       | Create a new editing session                   |
| `editing_sessions` | [Display edited data](graph-editing.md#display-edited-data)             | Highlight the objects modified in the session  |
| `editing_sessions` | [Drop editing session](graph-editing.md#drop-editing-session)           | Delete the session and its data                |
| `edges copie`      | [Finalize the roundabout](graph-editing.md#create-a-roundabout)         | Clean a roundabout digitized as a circle       |

They are used when the user would like to modify the Road graph. See the corresponding documentation.

To run an action on a feature, use the `Identify Features` tool and **right-click** on the feature in the map,
or use the `Run Feature Action` tool of the `Attributes` toolbar.

The Python code of these actions is stored in the plugin: the actions of the project only call it,
for example `plugins['roadnetwork'].run_action('drop_editing_session', ...)`.
