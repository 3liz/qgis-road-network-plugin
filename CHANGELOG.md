# Changelog

## Unreleased

## 0.4.6 - 2026-07-02

### Fixed

* Fix multiple misbehaviours of the SQL method `road_graph.update_managed_objects_on_graph_change`:
  * `road_graph.update_table_references_from_geometries`:
    fix error when managed object `road_code` does not exists
  * `road_graph.update_table_references_from_geometries`:
    Add an option to avoid setting the table object offset and side columns
  * `road_graph.update_managed_objects_on_graph_change`:
    always recalculate start & end points references before updating geometries
    so that the beginning and end of the linestring geometry does not move

## 0.4.5 - 2026-06-24

### Fixed

* SQL - Build MULTILINESTRING from start & end road references
  (function `road_graph.get_road_substring_from_references`):
  * fix bug during generation with unwanted intermediate `GEOMETRYCOLLECTION`
  * ensure the produced `MULTILINESTRING` parts are correctly ordered
    New function `road_graph.reorder_multilinestring_parts` which can be used
    in `UPDATE` SQL scripts

## 0.4.4 - 2026-06-19

### Fixed

* Update references from geometries
  * Force to use the object `road_code`
  * Set the objects references to `NULL` if no references have been found around 50m

## 0.4.3 - 2026-06-16

### Fixed

* **Administration dock** - Display the plugin schema version instead of the plugin version
* Algorithm **"Update managed objects"** - Fix the error when the feature geometry is empty
* SQL - Fix functions `update_table_references_from_geometries` & `get_merged_geom_from_table_and_ids`
  * `get_merged_geom_from_table_and_ids`: return `NULL` if the given table of Ids is empty
  * `update_table_references_from_geometries`:
    - get the geometry column name instead of using hard-coded `geom`
    - fix the wrong use of `mo.id`: replaced by the name of the primary key field
    - fix the calculation of `MULTILINESTRING` end points (hack by getting the last part)
* **QGIS admin project** - Fix wrong data source for the layer `managed_objects`

## 0.4.2 - 2026-06-15

### Changed

* SQL & algorithm - Calculate references from geometries : do not use
  columns `start_offset`, `start_side`, `end_offset`, `end_side` but only
  `offset` & `side` for linestrings (calculated from the start linestring node)

### Fixed

* Algorithm "Import data" (and related SQL function) - Do not add a marker 0
  if a marker for this road is already within 1m of an existing marker
* Algorithm "Update managed objects" - Fix issues when updating references for a linestring
  (fix function `road_graph.update_table_references_from_geometries`)
* SQL - Fix `NULL` or `0` calculated references while creating an edge with only 2 nodes

## 0.4.1 - 2026-06-05

### Fixed

* Fix the error occurring when updating managed objects references
  when the objects had a `cumulative` attribute

### Changed

* Minor changes on the QGIS project

### Tests

* Test the function `road_graph.merge_editing_session_data`
  and the impact on the managed object geometries or references

## 0.4.0 - 2026-05-22

### Added

* Import - Add SQL function & a new algorithm to import data from edges & markers template files
  (See example files `source_edges.fgb` & `source_markers.fgb` in plugin folder `/resources/import/`)
* SQL & QGIS - Create a view `v_managed_objects` to see the MultiPolygon geometry
  of the last updated objects of each managed table.

## 0.3.3 - 2026-04-17

### Changed

* Editing session - Update the managed objects when merging the editing session data
* Tests - Add more tests

### Fixed

* SQL - Avoid to cut edges at intersections outside the editing session polygon
* SQL - Separate methods to update geometries/references from the road graph

## 0.3.2 - 2026-04-12

### Fixed

* SQL
  * Fix the creation of roundabout by avoiding to insert edges which geometry
    already exists for another edge
  * Fix error when deleting a node (wrong merge edges attempt)

## 0.3.1 - 2026-04-11

### Added

* Locator : allow to use QGIS Locator bar (CTRL+K) with prefix `rr`
  Usage :
    * `D30` zoom to the full road
    * `D30 3 150.5 10 left` : zoom to the corresponding location
      (only some of the figures can be given)

### Changed

* Improve CI and packaging

## 0.3.0 - 2026-04-10

### Added

* Processing
  * Add algorithm allowing to update a layer references or geometries from the road graph
* Tests - Add test data & test some of the main features

### Changed

* Documentation
  * SchemaSpy: add functions
  * Fix schemaspy generation errors by using option `-t pgsql11`

### Fixed

* Fix deletion of an edge ending on a roundabout marker 0
* Fix wrong geometry generation for road substring (roads & roundabouts)
* SQL - Make sure 90_GLOSSARY.sql is created even if no glossary table is found

## 0.2.0

User interface:

* Toolbar
  - Change map tool behaviour: the user now needs to click on the map
    to get the references under the cursor instead of a simple hover.
  - add the shortcut `CTRL+MAJ+K` to activate/deactivate the map tool
  - New button to toggle the new tools dock
* New RoadNetwork tool QGIS panel:
  - Visualize the references of the `road_graph` and the `editing_schema` database schemas data
  - Add an checkbox to allow displaying the references when the mouse pointer moves
  - Add the possibility to search for a reference for the edited data:
    change a value and click on the `Find` button
* Editing session - Add an action available with right-clic on the editing session polygon
  to let the user drop the editing session and all its related data
* Work in progress - Prepare functions for applying graph changes to external data

SQL functions improvements and fixes:

* Performance - Improve the calculation of a point reference, especially for big roads
* Edges - Correctly update references of the new & old road when changing an edge `road_code` value
* Fix wrong update of edges references when a new edge is added or its `previous_edge_id` is changed
* Markers - Fix an error when deleting a marker
* Nodes - Add logic to control actions when a node is manually deleted

CI

* Migrate Transifex tools from qgis-plugin-ci to qt-transifex

## 0.1.0 - 2026-01-09

* First version of the plugin
