# How to edit the data

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
