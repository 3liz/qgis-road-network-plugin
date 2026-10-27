# Road graph editing

## Editing rules

The road graph data are **not edited directly** in the production schema `road_graph`,
but in a **dedicated editing session schema** `editing_session`, which acts as a sandbox:

1. The user **draws a polygon** in the `editing_sessions` layer around the area to edit.
   The new editing session has the status `created`.
2. The algorithm `Clone data to a new editing session` (toolbar button `Clone data to the editing sandbox`)
   **copies an extract of the production data** inside this polygon into the sandbox.
   The status becomes `cloned`.
3. The **editing** is made with the **QGIS tools**: forms, topological editing, etc.
   The status becomes `edited` after the first change.
    * QGIS **automatic transaction grouping** is activated in the project:
      the changes made by the database triggers (node creation, edge split, etc.) are visible **live**.
    * A map tool helps to **create roundabouts**.
4. The algorithm `Merge editing session data` (toolbar button `Merge editing sandbox data`)
   **merges the modifications back** into the `road_graph` schema
   and updates the [managed objects](managed-objects.md).
   The sandbox data and the merged editing session are then deleted.

![Editing in QGIS](../media/user-guide/qgis_editing.gif)

## How to edit the data in QGIS

In QGIS, you can edit the layer data by toggling the **editing mode** for each layer:

* **Select the layer** in the panel by clicking on it: the layer is highlighted in blue
* **Right-click on the layer name** and select `Toggle Editing` or use the menu `Layer / Toggle Editing`

To **add a new record**, select the layer in the `Layers` panel,
then use the menu `Edit / Add Record` (or `CTRL+.`). It will open a form which lets you enter the needed data.
Once every required fields have been filled, validate with the `OK` button on the bottom-right.

To **view all the records** of a layer, select the layer in the `Layers` panel,
then open the attribute table with the menu `Layer / Open Attribute Table` (or `F6`)

You can **undo previous modifications** with the menu `Edit / Undo` (or `CTRL+Z`).

To permanently **save the changed data** in the layer, you need to use the menu `Layer / Save Layer Edits`.
After saving the data, you will not be able to undo your changes
(but you can always reopen the data and changed the values).

To **edit a record**, open the layer attribute table, and then click on the small button `Switch to form view`
on the bottom-right of the dialog (the first one). Then select a record in the left panel,
and use the displayed form to edit the data.

To **delete a record**, select the record in the attribute table by clicking on the line number on the left:
the line must be highlighted in blue. Then use the trash icon `Delete selected features` (or `Del`).
*You can undo the deletion if you have not yet saved layer edits*.
