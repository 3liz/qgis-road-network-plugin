# Changelog

## Unreleased

## 0.3.1-alpha.1 - 2026-04-10

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
