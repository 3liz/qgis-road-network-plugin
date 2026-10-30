# Changelog

## Unreleased

### Changed

* **SQL**: performance of the edges editing: the references of a road were calculated several times
  for one modification. Splitting an edge by a node now calculates them once instead of three times,
  and adding an edge to a road once instead of twice. For example, drawing an edge crossing twice
  a long road calculates about 3 times fewer references (migration `upgrade_to_41.sql`)
* **Docs**: in the page `Road graph editing`, warn against editing directly the production schema `road_graph`,
  and explain how to correct the whole road graph after a first import with an editing session covering all the data

## 0.6.1 - 2026-10-29

### Summary

**Highlights**

* **GPS tracks**: new Processing algorithm to get the road sections followed by the lines of a layer,
  for example GPS tracks, with their start and end references
* **Performance**: modifying a field of the edges is much faster

**Upgrade notes**

* Run the algorithm `Upgrade database structure` to upgrade the database (migration 40)

### Added

* **Processing**: new algorithm `Get road sections from a line (GPS track)` in the group `Linear referencing`,
  which snaps the lines of a layer, for example GPS tracks, on the road graph,
  and creates a layer with the sections of the roads they follow, with their start and end references

### Changed

* **SQL**: performance of the edges updates: the references of an updated edge are not calculated anymore
  by the trigger `before_edge_insert_or_update` when they are calculated again just after
  for the whole road, or when only another field of the edge has been modified.
  For example, modifying a field of all the edges of a road is a few hundred times faster (migration `upgrade_to_40.sql`)
* **SQL**: new function `road_graph.get_or_create_edge_node`, used by the trigger `before_edge_insert_or_update`
  to get or create the start and end nodes of an edge, instead of duplicated code

### Fixed

* **SQL**: the search of the next edge of an edge could fail if two edges of the same road started at the same node

## 0.6.0 - 2026-10-28

### Summary

**Highlights**

* **GPS tracks**: snap a GPS track, for example from a GPX file, on the road graph,
  and get the references of each road it follows (new SQL functions)
* **Performance**: the calculation of the references is about 9 times faster for the edges,
  and about 3.5 times faster for the managed objects
* **Road graph editing**: the field `no_intersection_cutting` is respected by all the trigger functions,
  drawing an edge across a highway interchange no longer hangs, and no orphan node is created anymore
* **Managed objects**: new functions to add the reference fields and the triggers calculating the references,
  materialized view of the last updated objects, and fixed split of a line by a point in Lizmap
* **Documentation**: reorganized and completed: Processing algorithms, tables and fields, functions examples,
  demo database, toolbar and panels, training plan

**Upgrade notes**

* Run the algorithm `Upgrade database structure` to upgrade the database (migrations 31 to 39)
* The editing schema `editing_session` is renamed `road_graph_editing`: recreate the QGIS administration project
  with the algorithm `Create database local interface`, or update the data sources of the existing projects
* The Python code of the layer actions is now stored in the plugin: the plugin must be installed and activated
  to use the actions of the administration project

### Added

* **SQL**: add the function `road_graph.add_managed_object_reference_fields` which adds to a managed object table
  the reference fields corresponding to its geometry type (migration `upgrade_to_39.sql`)
* **SQL**: add the function `road_graph.add_managed_objects_triggers()` which creates the trigger
  `trg_calculate_managed_object_references` on every table listed in `road_graph.managed_objects`
  with the new column `add_trigger_on_geom_change` set to `True`, if it does not already exist
* **SQL**: add the materialized view `road_graph.vm_managed_objects`, based on the view `v_managed_objects`
  and refreshed at the end of the function `merge_editing_session_data`.
  The QGIS administration project uses it instead of the view `v_managed_objects`
* **SQL**: add the functions `road_graph.get_snapped_line_from_track` which snaps an imprecise GPS track
  (for example from a GPX file) on the road graph edges, without zigzags between close roads,
  and `road_graph.get_road_sections_from_snapped_line` which returns the ordered list of the roads
  followed by the snapped line, with their start and end references (migration `upgrade_to_34.sql`)
* **Docs**: the `Database` menu now contains the pages `Useful functions`,
  which explains how to use these functions, and `Database structure`, which embeds the SchemaSpy documentation
* **Docs**: explain the use of the field `no_intersection_cutting` in the page `Road graph editing`

### Changed

* **Docs**: reorganization and new content of the documentation:
  new pages `Processing algorithms` and `Tables and fields`,
  description of the toolbar and panels of the plugin, examples of the linear referencing functions,
  creation of a demo database, example of the impacts of a graph modification on the business data,
  the preparation of the business tables moved to the page `Managed objects`,
  and a training plan in appendix
* **QGIS project**: the Python code of the layer actions has been moved into the plugin
  (module `roadnetwork/actions`). The actions of the administration project now only call
  `plugins['roadnetwork'].run_action(...)`: `finalize_roundabout` (layer `edges copie`),
  `display_edited_data`, `create_editing_session` and `drop_editing_session` (layer `editing_sessions`)
* **Processing**: remove the `Schema name` parameter from the algorithms `Create database structure`
  and `Upgrade database structure`: the plugin schema is always `road_graph`
* **SQL**: rename the editing schema `editing_session` to `road_graph_editing`
  (migration `upgrade_to_32.sql`). The table `road_graph.editing_sessions` and the functions
  `*_editing_session*` keep their names. The QGIS administration project and the plugin tools
  use the new schema name: QGIS projects created before must be recreated
  with the algorithm `Create database local interface`, or their data sources updated

### Fixed

* **Processing**: in the algorithm `Update managed objects from the road graph`, the option
  `Preserve start & end points when updating line geometries` had no effect
* **QGIS**: in the panel `RoadNetwork tools`, pressing `Enter` in a field did not run the search `Find`
* **QGIS**: the error message of the tool `Get references under the cursor` raised an error itself
* **Processing**: the help of the algorithm `Merge editing session data` was the help of the algorithm cloning the data
* **SQL**: the geometry of an editing session which status is not `created` could still be modified
  by setting its status back to `created` in the same query, or by removing it (migration `upgrade_to_38.sql`)
* **Lizmap**: in the references form of the road network panel, the button `Road` of the start references
  went to the end of the road instead of its start
* **SQL**: split of a managed object linestring by a point (Lizmap action `split_line_by_point`):
  the references were not updated when the table has no trigger calculating them.
  The first part now ends, and the second part starts, at the references of the cut point.
  A feature digitized in the opposite direction of the road can now also be split (migration `upgrade_to_37.sql`)
* **SQL**: drawing an edge across edges crossing without node (bridges, highway interchanges) never ended:
  the parts of each split edge created nodes at all their intersections, which were propagated
  to the edges crossing them, and so on. Only the intersections of the drawn edge are now cut
* **SQL**: when a new edge crossed several edges, a second node, not linked to any edge,
  could be created 10 cm away from a crossing node (rounding of the coordinates of the split parts)
* **SQL**: the edges with `no_intersection_cutting` set to `True` were still cut by the nodes located on them,
  for example a new edge with this value passing over an existing crossroad.
  They are now only cut when the start or end point of a created or moved edge is located on them,
  for example a new access road arriving in the middle of a highway edge (migration `upgrade_to_36.sql`)
* **SQL**: performance of the references calculation: the recalculation of the edges references
  of all the roads (`update_edge_references`) is about 9 times faster, with the same results.
  With the road cache, the previous marker is only searched among the markers of the road
  (all the markers of the database were read), and the edge order is stored in the cache
  instead of being calculated again for each point
* **SQL**: performance of the update of the managed objects references (`update_table_references_from_geometries`),
  about 3.5 times faster on long roads: the road cache can now contain several roads,
  and is built for the roads of the objects before calculating their references
* **SQL**: `get_ordered_edges` returned twice the edge of the roads having only one edge
* **SQL**: when the first edge of a road is updated, for example when it is split by a new node,
  do not create a marker `0` if the road already has markers. Some roads do not start with the marker `0`
  (e.g. a road starting at the marker `4 + 918`): a wrong marker `0` was created, or the editing failed
  with the error `The object geometry must intersect the editing session polygon`
  if the start of the road was outside the editing session (migration `upgrade_to_33.sql`)
* **SQL**: the session variables `road.graph.*` used by the topology triggers are set with `SET LOCAL`,
  so that their value cannot leak outside of the current transaction (migration `upgrade_to_33.sql`)
* **SQL**: remove the `SET search_path` / `RESET search_path` inside the functions
  `drop_editing_session`, `editing_survey` and `split_edge_by_node`, which now use the quoted schema name
  `"road_graph"`. The function `merge_editing_session_data` has its `search_path` set in its definition
* **Locator**: remove the prefix `rr`, which was ignored by QGIS
  (plugin prefixes must have at least 3 characters).
  The search is run without typing any prefix.

## 0.5.7 - 2026-10-22

### Added

* **SQL**
  * Add a **trigger function** to update a **managed object table references** on insert or update
    To use it, the table must be configured in `road_graph.managed_objects`. Then create the trigger with:
    ```sql
    CREATE TRIGGER trg_calculate_managed_object_references
    BEFORE INSERT OR UPDATE OF geom
    ON public.demo_painting
    FOR EACH ROW EXECUTE PROCEDURE road_graph.calculate_managed_object_references();
    ```
  * Add **functions** `road_graph.merge_table_linestring_features` & `road_graph.split_table_linestring_feature_by_point`
    which are able to **merge two features** of the same table & **split a feature** by a given point
* **Docs - Lizmap Web Client Javascript & CSS files**
  * Add buttons to go to the **start/end of the route**
  * Add a popup button to let the user **split the linestring** with the clicked point.
* **QGIS Project**: layer `Roads` - Add expression based `road_order` colummn

### Fixed

* **SQL** - `road_graph.merge_editing_session_data`: fix search_path not well resetted
* **Docs - Lizmap Web Client - Javascript**:
  * force the `road_code` value to be used when creating geometries
  * use the `road_order` column for the autocompletion data source

### Changed

* Docs - **Schemaspy**: add functions SQL definition


## 0.5.6 - 2026-09-29

### Fixed

* **Translation** - Fix loading the language file

## 0.5.5 - 2026-09-29

### Fixed

* **Translation** - Fix the path of the language files so that the French translation
  is correctly named and used by the plugin

### Changed

* **Edges & markers**:
  * Add a new column id_source to keep the reference of the imported data identifier
  * Update the QGIS administration template project accordingly
* **Tests**
  * Import updated test data & update example files in the folder `roadnetwork/resources/import`
  * Update all the Python tests with these new data
  * Fix the test comparing edges data before & after we merge the editing session data
* **Docs** - Improve the Lizmap Web Client **Javascript** and **CSS** files containing
  the tools to use the Road Graph PostgreSQL functions.
  An example of the Lizmap **JSON action file** has also been added in `docs/media`
* **SQL Function** `road_graph.get_reference_from_point`: use a 5m tolerance for getting the next
  edge references when the point is floating between two unconnected edges


## 0.5.4 - 2026-09-09

### Changed

* **Hover map tool** - use QTimer to limit requests & fix bug with `editing_sessions` layer QGIS action
* **Get point reference** - Use the next edge start marker & abscissa for calculating metrics for points
  located between two unconnected edges of the same road
* **Tests** - Add a make pull command to pull docker images

### Added

* **SQL** - `road_graph.get_road_substring_from_references`: add new parameters
  to prepend and/or expand the closest edge geometry at the start and/or the end
  of the calculated road substring geometry.


## 0.5.3 - 2026-07-30

### Changed

References & geometry calculation: be more tolerant for limit cases

* Function `road_graph.update_table_references_from_geometries`:
  be more tolerant for **roundabout** & invert start & end result if needed
* Function `road_graph.get_road_substring_from_references`:
  Add information when raising exception to help find the problematic objects
* Minor fixes: replace wrong usage of `editing_session` schema in 2 functions

### Fixed

* SQL Function `road_graph.get_reference_from_point`:
  fix exception occurring with distance calculation (PostGIS Bug)

## 0.5.2 - 2026-07-28

### Fixed

* **Import** - SQL function `road_graph.import_data_from_template_tables`:
  Fix error when importing more than one dataset. The function
  used the original data `id` instead of getting the sequence next value.

## 0.5.1 - 2026-07-28

### Fixed

* **SQl** - function `road_graph.get_downstream_multilinestring_from_reference`
  Fix wrong result when the closest edge substring from the given marker projected point
  to the end of the road. When the result is `EMPTY` (for example because the marker
  is exactly at the end of the edge), the function now returns a valid non-empty
  linestring (formed by the edge end point doubled)

## 0.5.0 - 2026-07-27

### Added

* Add the support for other Coordinate References System (CRS) than `EPSG:2154`
  * Default CRS is still `EPSG:2154`
  * Add a new CRS input in the algorithms **Create database structure**
  * Adapt the algorithms **Upgrade database structure** & **Import data** to use
    the database SRID (taken from the `geometry_columns` PostGIS view)

## 0.4.11 - 2026-07-27

### Changed

* SQL `road_graph.get_road_substring_from_references`: Add specific calculations for roundabout.
  Control the values of start abscissa and end abscissa and set them to the beginning or end
  of the road to avoid errors.


## 0.4.10 - 2026-07-24

### Changed

* `get_road_substring_from_references`: adapt the start & end input values
  from the road markers:
  * always go to the end of the road when end marker code and/or end abscissa
    are bigger than possible
  * if the start marker code is below the minimum code for the road,
    use the latter instead with a start abscissa of 0
* `update_table_references_from_geometries`: Do not change the value of the column
  `road_code` if no references have been found.

* Tests:
  * Fix wrong database connection used for local tests && Improve documentation
  * Fix the test `test_upgrade_from`: start from version 1.

## 0.4.9 - 2026-07-22

### Changed

* **Edges editing** - Replace field `z_level` by `no_intersection_cutting` (boolean).
  This new field has been added as a way to prevent the trigger functions
  from automatically cutting the edges having this value set to True.
  It is useful when digitizing motorway access roads, edges located under a bridge, etc.
  **You need to recreate your administration project to reflect this change**
* SQL - Improve the function `road_graph.toggle_foreign_key_constraints`
  so that the import and merge editing session algorithms can be used by non-superuser roles

## 0.4.8 - 2026-07-10

### Changed

* Processing algorithm - `Update managed objects` : .
  add an option to preserve the linestrings start & end positions.
  When using this option combined with the update policy `Update geometries`,
  the references of the existing geometries will be calculated for the start & end
  line nodes, and then the geometry will be updated from these new references.
* SQL - Trigger function `road_graph.after_edge_insert_or_update()`: do not create node at intersections for different z_level
* QGIS admin project - Add the field `z_level` for edges layers which
* Plugin code: Use `psycopg2` rather than f-string to build SQL queries.
  (This will allow the plugin to be pushed to QGIS official repository)

## 0.4.7 - 2026-07-06

### Added

* Lizmap Web Client : add Javascript & CSS files to use with a RoadNetwork published project

### Fixed

* SQL fixes and improvements
  - `road_graph.get_road_substring_from_references`:
    * Check if the resulted geometry has the correct orientation and reverse it if needed
      (the function `ST_OffsetCurve` does not preserver orientation for MULTILINESTRINGS)
    * Change the precision rounding of generated geometries from `0.01` to `0.1`
    * Use default `0.0` & `right` values for features with empty columns `offset` and `side`
  - `road_graph.update_managed_objects_on_graph_change`: fix error when no object has been updated

### Changed

* Tests & CI - Improvements & update PostgreSQL version from 15 to 17
* Doc - Add draft concepts to translate

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
