# Introduction

To go on, you must first have installed and configured the plugin for QGIS Desktop.
See the doc [installation and configuration](../installation/)

## Create your database local interface

This algorithm will create a **new QGIS project file** for administration purpose.

The generated QGIS project must then be opened by the administrator
to create the needed metadata by using QGIS editing capabilities

Parameters:

* `PostgreSQL connection to the database`: name of the database connection
  you would like to use for the new QGIS project.
* `QGIS project file to create`: choose the output file destination.
  We advise you to choose a name reflecting the database name.

## Edit the database metadata

### The administration project

The **administration project** created beforehand will allow the administrator
to **create, modify and delete metadata** in the database. As an administrator,
you must open this project with QGIS.

Before going on, be sure you have a working connection between your computer and the database server.

Once opened, the QGIS project is configured to allow the administrator to edit the data.


### How to edit the data

In QGIS, you can edit the layer data by toggling the **editing mode** for each layer:

* Select the layer in the panel by clicking on it: the layer is highlighted in blue
* Right-click on the layer name and select `Toggle Editing` or use the menu `Layer / Toggle Editing`

Before going on, please **toggle editing** for all the following layers:

* test

To **add a new record**, select the layer in the `Layers` panel,
then use the menu `Edit / Add record` (or `CTRL+.`). It will open a form which lets you enter the needed data.
Once every required fields have been filled, validate with the `OK` button on the bottom-right.

To **view all the records** of a layer, select the layer in the `Layers` panel,
then open the attribute table with the menu `Layers / Open attribute table` (or `F6`)

You can **undo previous modifications** with the menu `Edit / Undo` (or `CTRL+Z`).

To permanently **save the changed data** in the layer, you need to use the menu `Layer / Saver layer edits`.
After saving the data, you will not be able to undo your changes
(but you can always reopen the data and changed the values).

To **edit a record**, open the layer attribute table, and then click on the small button `Switch to form view`
on the bottom-right of the dialog (the first one). Then select a record in the left panel,
and use the displayed form to edit the data.

To **delete a record**, select the record in the attribute table by clicking on the line number on the left:
the line must be highlighted in blue. Then use the trash icon `Delete selected features` (or `Del`).
*You can undo the deletion if you have not yet saved layer edits*.

### Example data

#### sample_table

Example content:

| id | ac_label             | ac_description |
|----|----------------------|----------------|
| 1  | Public organizations |                |
| 2  | Research centers     |                |


Notes:

* **Id** will be automatically given after saving layer edits. Do not modify.
* **Label** and **description** are mandatory
