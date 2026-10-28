# Processing algorithms

The plugin adds the provider **RoadNetwork** to the QGIS **Processing toolbox** (menu `Processing / Toolbox`).
Most of the algorithms are also available with the buttons of the [toolbar and panels](qgis-project.md#toolbar-menu-and-panels).

| Group | Algorithm | Usage |
|-------|-----------|-------|
| Configuration | [Configure plugin](#configure-plugin) | Choose the PostgreSQL connection of the project |
| Structure | [Create database structure](#create-database-structure) | Install the schema `road_graph` in the database |
| Structure | [Upgrade database structure](#upgrade-database-structure) | Upgrade the schema after a plugin upgrade |
| Administration | [Create database local interface](#create-database-local-interface) | Create the QGIS administration project |
| Editing | [Import data from template](#import-data-from-template) | Fill an empty road graph from edges and markers layers |
| Editing | [Clone data to a new editing session](#clone-data-to-a-new-editing-session) | Copy the data of an editing session area into the sandbox |
| Editing | [Merge editing session data](#merge-editing-session-data) | Merge the sandbox data into the production data |
| Editing | [Update managed objects from the road graph](#update-managed-objects-from-the-road-graph) | Calculate the references from the geometries, or the geometries from the references, of a layer |

All the algorithms have a parameter `PostgreSQL connection to the database`:
by default, the connection chosen with the algorithm `Configure plugin`.

## Configuration

### Configure plugin

Choose the **PostgreSQL connection** used by the current QGIS project.
It must be run before the other algorithms.
See [Configure the plugin](../installation/index.md#configure-the-plugin).

## Structure

### Create database structure

Install the schema `road_graph`, with its tables, views and functions, and the editing schema `road_graph_editing`.
See [Create the database structure](../installation/index.md#create-the-database-structure).

### Upgrade database structure

Upgrade the database structure to the version of the plugin, after a plugin upgrade.
The administration panel shows if an upgrade is needed.
See [Upgrade database structure](../installation/index.md#upgrade-database-structure).

## Administration

### Create database local interface

Create a **QGIS administration project** with all the layers, styles, labels and actions.
See [Create your database local interface](../installation/index.md#create-your-database-local-interface)
and [Administration project](qgis-project.md#administration-project).

## Editing

### Import data from template

Fill an **empty road graph** from an edges layer and a markers layer.
The roads, nodes, edges and markers are created, and the references of the edges are calculated.
See [Import the road graph](import.md#import-the-road-graph-edges-and-markers).

### Clone data to a new editing session

Copy the production data located inside the **last editing session polygon** with the status `created`
into the editing schema `road_graph_editing`, the sandbox. The status of the session becomes `cloned`.

Also available with the toolbar button `Clone data to the editing sandbox`
and with the action `Create editing session` of the `editing_sessions` layer.

The algorithm refuses to run if:

* the `editing_sessions` layer is in editing mode: save the changes and stop the editing beforehand,
* an editing session with the status `edited` has not been merged or dropped yet,
* there is no editing session with the status `created`: draw the editing session polygon beforehand.

![Clone data to a new editing session](../media/user-guide/processing_create_editing_session.png)

See [Road graph editing](graph-editing.md#editing-rules).

### Merge editing session data

Merge the data of the **editing session with the status `edited`** into the production schema `road_graph`,
update the [managed objects](managed-objects.md#update-rules) impacted by the changes,
then empty the sandbox and delete the merged editing session.

Also available with the toolbar button `Merge editing sandbox data`.

The algorithm refuses to run if the `editing_sessions` layer cannot be found or is in editing mode,
or if there is no editing session with the status `edited`.

![Merge editing session data](../media/user-guide/processing_merge_editing_session.png)

See [Road graph editing](graph-editing.md#editing-rules).

### Update managed objects from the road graph

Update the **references** or the **geometries** of the features of a vector layer from the road graph.
The layer can be a PostgreSQL table of the database, or any other vector layer, for example a GeoPackage layer.

Parameters:

* `Input vector layer`: a point or (multi)linestring layer,
* `Update policy`:
    * `Update references`: the references of each feature are calculated from its geometry,
    * `Update geometries`: the geometry of each feature is calculated from its references,
* `Update geometries - Preserve start & end points when updating line geometries`: `No` or `Yes`,
* `Output layer`: the layer created with the updated features.

![Update managed objects from the road graph](../media/user-guide/processing_update_managed_objects.png)

The input layer must have the **reference fields** corresponding to its geometry type:

* point layers: `road_code`, `marker_code`, `abscissa`, and optionally `offset`, `side` and `cumulative`,
* linestring layers: `road_code`, `start_marker_code`, `start_abscissa`, `end_marker_code`, `end_abscissa`,
  and optionally `offset` and `side`.

The algorithm supports the **in-place editing** of the input layer: select the layer, toggle its editing mode,
then use the button `Edit Features In-Place` of the Processing toolbox.
The features are then updated directly in the layer, and the changes can be checked before saving them.

For the tables registered in the `managed_objects` layer, the same updates can be done with SQL,
see [Calculate the references](managed-objects.md#calculate-the-references).
They are also done automatically when an editing session is merged, see [Update rules](managed-objects.md#update-rules).
