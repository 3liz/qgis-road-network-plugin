--
-- PostgreSQL database dump
--






SET statement_timeout = 0;
SET lock_timeout = 0;


SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- FUNCTION aa_before_geometry_insert_or_update()
COMMENT ON FUNCTION road_graph.aa_before_geometry_insert_or_update() IS 'Fonction qui arrondit la précision des coordonnées à 0.1 soit 10cm si ce n''est pas déjà fait
lors d''une création ou d''une modification de géométrie.
Elle est préfixée par aa_ pour être lancée avant les autres trigger,
car l''ordre alphabétique compte.
';


-- FUNCTION aa_before_record_insert_or_update()
COMMENT ON FUNCTION road_graph.aa_before_record_insert_or_update() IS 'Set the created_at and updated_at fields';


-- FUNCTION add_managed_object_reference_fields(_schema_name text, _table_name text, _add_optional_fields boolean)
COMMENT ON FUNCTION road_graph.add_managed_object_reference_fields(_schema_name text, _table_name text, _add_optional_fields boolean) IS 'Add the reference fields needed by the managed objects functions to the given table, depending on its geometry type.
The geometry type is read from the table road_graph.managed_objects if the table is registered, or else from its geometry column geom.
* all the geometry types: road_code (text), and the optional fields offset (real) and side (text),
* point: marker_code (integer), abscissa (real), and the optional field cumulative (real),
* linestring and multilinestring: start_marker_code, end_marker_code (integer), start_abscissa, end_abscissa (real),
  and the optional fields start_cumulative, end_cumulative (real).
The optional fields are added only if the parameter _add_optional_fields is True (default).
The existing fields are not modified. The function returns one line per field with its name, type and status:
added, already exists, or already exists with the type of the existing field.
If the fields cannot be added, it returns one line with the status: table not found, geom column not found,
or geometry type not supported.';


-- FUNCTION add_managed_objects_triggers()
COMMENT ON FUNCTION road_graph.add_managed_objects_triggers() IS 'Create the managed objects triggers on each table listed in road_graph.managed_objects
with the column add_trigger_on_geom_change set to True.

Each trigger is only created if it does not already exist:

* trg_calculate_managed_object_references, for all the geometry types:
  it calculates the references of the inserted or updated features
  with the function road_graph.calculate_managed_object_references,
* trg_update_managed_object_linked_geometries, only for the linestring and multilinestring tables:
  it updates the lines connected to an updated line
  with the function road_graph.update_managed_object_linked_geometries.

It returns one line per trigger with the schema name, table name, trigger name and trigger status:
created or already exists.

If no trigger can be created for a managed object, it returns one line with an empty trigger name
and the status: not requested, table not found or geom column not found.';


-- FUNCTION after_edge_delete()
COMMENT ON FUNCTION road_graph.after_edge_delete() IS 'Lors de la suppression d''un tronçon, on supprime les Nodes qui étaient rattachés aux sommets amont et aval du tronçon,
uniquement si ces derniers ne sont plus rattachés à aucun autre tronçon.
On fusionne aussi les tronçons collés plus liés par les possibles noeuds supprimés
';


-- FUNCTION after_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.after_edge_insert_or_update() IS 'Multiples opérations lancées suite à la modification d''un troncon.
Déplacement du noeud initial et terminal liés si besoin.
Suppression des noeuds orphelins si besoin.
Création des noeuds non existants à l''intersection avec les autres edges
seulement pour les edges de même niveau
';


-- FUNCTION after_marker_insert_or_update_or_delete()
COMMENT ON FUNCTION road_graph.after_marker_insert_or_update_or_delete() IS 'Update road edges references after a marker has been created, updated or deleted.';


-- FUNCTION after_node_insert_or_update()
COMMENT ON FUNCTION road_graph.after_node_insert_or_update() IS '1. Lors d''une création de Node (automatique via une action sur les tronçons) ou de la modification de la position d''un Node
(donc lorsque la géométrie change), on met à jour la géométrie des tronçons dont un des sommets (amont ou aval) est rattaché à ce Node,
et on renseigne les champs start_node et end_node (id des Nodes amont et aval) des tronçons concernés.

2. Lors de la création ou d''une modification de la géométrie d''un tronçon,
si un des sommets (amont ou aval) du tronçon accroche le segment d''un tronçon existant :
Ce tronçon existant est coupé en deux (la géométrie du tronçon A est tronquée, un nouveau tronçon B est créé, on récupère les attributs du tronçon A dans le tronçon B).
Rq :
- La fonction split_edge_by_node a été créée et est appelée à cet effet
- La création du Node à cette nouvelle intersection est gérée séparément dans un trigger sur les tronçons
';


-- FUNCTION before_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.before_edge_insert_or_update() IS 'During the creation or modification of an edge, we verify that the upstream and downstream nodes exist
within 50 cm of the start and end of the edge. If they do, we use them
and update the edge geometry so that the start and end are exactly on these nodes.
Otherwise, we create the missing nodes.
Additionally, during the creation of an edge, if the road code is provided and no marker 0 exists for this road,
we automatically create one at the start of the edge.
During the modification of an edge, if the starting point of the edge is modified and the marker 0 of the road is positioned at this starting point,
we move marker 0 to this new starting point.
When the geometry of an edge is only reversed, its start and end nodes are swapped,
its previous and next edges are calculated again, and the marker 0 is not moved.
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';


-- FUNCTION before_editing_sessions_update()
COMMENT ON FUNCTION road_graph.before_editing_sessions_update() IS 'Prevent from updating an editing session geometry if its status is not created, which means that data has been cloned into the road_graph_editing schema';


-- FUNCTION before_node_delete()
COMMENT ON FUNCTION road_graph.before_node_delete() IS 'When a node is deleted, we update all the edges linked to it to keep the graph consistent.
We also merge edges if the deleted node was between two edges.
';


-- FUNCTION build_road_cached_objects(_road_code text)
COMMENT ON FUNCTION road_graph.build_road_cached_objects(_road_code text) IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
* the order of the edges, used by get_reference_from_point to avoid calculating it again for each point

Cached data is stored in the temporary table road_cache, with one row per road, living only until COMMIT.
Each call replaces the row of the given road: the cache of a road must be built again after any modification
of its edges or markers in the same transaction, before using it.
The functions using the cache fall back to the calculation without cache if the road is not in the cache.
';


-- FUNCTION calculate_managed_object_references()
COMMENT ON FUNCTION road_graph.calculate_managed_object_references() IS 'Trigger function allowing to update the references of an inserted or update managed object feature.

For a line, the start and end references follow the road direction (start before end),
even if the line is digitized in the opposite direction of the road.';


-- FUNCTION check_graph(_checks text[], _road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph(_checks text[], _road_codes text[], _options jsonb) IS 'Run the checks of the road graph and return one line per issue found.

The checks are listed in the table glossary_graph_check of the production schema.
All the active checks are run if _checks is NULL, and all the roads are checked if _road_codes is NULL.
The parameter _options overrides the default options of the checks stored in this table,
with the check code as key: {"G02": {"tolerance": 2}}.

Each line gives the check code and title, the severity (error or warning), the road code,
the ids of the edges, nodes and markers concerned, some details, the action which can fix the issue
with its parameters (NULL if the issue must be fixed manually), a point locating the issue (geom)
and, if useful, a line showing it (geom_detail).

The function can be run on the production schema or on the editing schema: road_graph_editing.check_graph()
checks the data of the editing session. The issues must be fixed in an editing session, with the function fix_graph_issue.';


-- FUNCTION check_graph_close_nodes(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_close_nodes(_road_codes text[], _options jsonb) IS 'Check G03 run by the function check_graph: find the nodes close to each other, which should be the same node.

An issue is returned for each pair of nodes less than the tolerance from each other: option tolerance, 1 m by default.
The nodes linked by an edge are ignored: this edge is very short, and is found by another check.

The node to keep is the node with the most edges, so that the fewest edges are moved,
or else the node with the lowest id, i.e. the oldest node: it is the first node of node_ids,
and the point of the issue. The details give the road codes of the edges of the two nodes, and the distance.
The issue can be fixed with the action merge_nodes, which moves the edges of the other node to the node to keep.';


-- FUNCTION check_graph_crossing_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_crossing_edges(_road_codes text[], _options jsonb) IS 'Check G01 run by the function check_graph: find the edges crossing each other without being split by a node.

An issue is returned for each crossing point which is not at the start or end point of both edges (50 cm tolerance,
like the triggers splitting the edges). The edges with the field no_intersection_cutting set to True,
for example on a bridge, are ignored.

The details give the crossing type, the road codes and classes of the two edges, and the angle between them,
which help to find the bridges:

* touching: the point is the start or end point of one of the edges, for example a road arriving on another road.
  The edges are surely connected: the fix action is cut_edges, which creates a node at the point and splits the edges,
* crossing: the edges cross each other, maybe on a bridge or in a tunnel. The fix action is given by the option
  crossing_fix_action of the check, NULL by default: it must be chosen for each issue, cut_edges if the roads
  are connected, or mark_as_bridge, which sets no_intersection_cutting to True for the edges.';


-- FUNCTION check_graph_parallel_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_parallel_edges(_road_codes text[], _options jsonb) IS 'Check G07 run by the function check_graph: find the edges of the same road running parallel to each other,
for example the two carriageways of a road using the same road code.

An issue is returned for each pair of edges of the same road, not connected by a node, which run along each other
less than the option tolerance from each other (30 m by default), along at least the option min_length (10 m by default).
The edges which only approach each other, for example on both sides of a roundabout, are ignored. The roundabouts are ignored.
The details give the distance between the edges and the length along which they are parallel.
The line of the issue shows the parallel parts of the edges.

The issue must be fixed manually, for example by giving another road code to one of the carriageways.';


-- FUNCTION check_graph_reversed_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_reversed_edges(_road_codes text[], _options jsonb) IS 'Check G05 run by the function check_graph: find the edges in the opposite direction of their road.

The edges of a road are grouped in sequences: two edges follow each other at a node where there are only
these two edges of the road. The direction of each sequence is given by the order of the markers of the road
located along it (less than the option marker_tolerance from it, 10 m by default), or else by the direction
of most of the length of its edges. The markers of the roundabouts are not used.
The direction of the edges is given by their geometry, not by their start and end nodes.

An issue is returned for each group of consecutive edges in the opposite direction of their sequence.
The details give the reason (markers or neighbours), the number of edges and their length,
and the number of consecutive markers in the direction of the road and in the opposite direction.
The issue can be fixed with the action reverse_edges, which reverses the geometry of the edges.';


-- FUNCTION check_graph_road_branches(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_road_branches(_road_codes text[], _options jsonb) IS 'Check G06 run by the function check_graph: find the roads with branches, for example the ramps
or the carriageways of a road using the code of the road.

An issue is returned for each node with more than two edges of the same road. The roundabouts are ignored.
The main line of the road at the node is given by the edge arriving at the node and the edge leaving it
which are the most aligned: the other edges are the branches. The details give the number of edges at the node,
and the ids of the edges of the main line and of the branches. The line of the issue shows the branches.

The issue must be fixed manually, for example by giving another road code to the edges of the branches.';


-- FUNCTION check_graph_short_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_short_edges(_road_codes text[], _options jsonb) IS 'Check G08 run by the function check_graph: find the short edges, often coming from the digitizing,
for example two junctions a few decimeters from each other.

An issue is returned for each edge shorter than the option min_length (3 m by default).
The edges of the roundabouts and the edges arriving at a roundabout or leaving it are often short: they are ignored,
unless they are reduced to a point (less than 10 cm). The severity is error for an edge reduced to a point, else warning.
The details give the length, the number of other edges at the start and end nodes, and if the edge is near a roundabout.

The issue can be fixed with the action collapse_edge, which deletes the edge and merges its nodes,
adding its geometry to the next or previous edge of the same road: the node to keep is the node with the most edges, or else the node with the lowest id,
and is the first node of node_ids. The only edge of a road has no fix action.';


-- FUNCTION check_graph_t_junction_gaps(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_t_junction_gaps(_road_codes text[], _options jsonb) IS 'Check G02 run by the function check_graph: find the edges ending near another edge without being connected to it,
for example a road arriving on another road and stopping a few decimeters before it (T junction).

An issue is returned for each free end of an edge (no other edge is connected to it)
which is less than the tolerance from another edge: option tolerance, 2 m by default.
The edges connected to the opposite end of the edge are ignored, for example its next edge if it is very short,
and the edges crossed by the edge near its free end: the edge goes beyond the other edge, which is found by the check G01.
When two free ends are close to each other, only one issue is returned.

The point to join is the closest point of the other edge, or its start or end point if it is less than 50 cm from it.
The details give the road codes of the two edges, the distance, and the join type: edge or edge_end.
The issue can be fixed with the action extend_edge, which adds the point to join at the free end of the edge.';


-- FUNCTION check_graph_useless_nodes(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_useless_nodes(_road_codes text[], _options jsonb) IS 'Check G04 run by the function check_graph: find the useless nodes, which can be deleted.

An issue is returned for each node without edge (only when all the roads are checked),
and for each node between two edges of the same road which can be merged: one edge arrives at the node
and the other one leaves it, with the same value of no_intersection_cutting, they do not form a loop,
and they only touch each other at the node.
The details give the node type (no_edge or two_edges) and the road code of the edges.

The issue can be fixed with the action delete_node, which deletes the node: the triggers merge the two edges.';


-- FUNCTION clean_digitized_roundabout(_road_code text)
COMMENT ON FUNCTION road_graph.clean_digitized_roundabout(_road_code text) IS 'Clean a roundabout digitized by QGIS circle tool:
* delete the edges inside the roundabout
* remove the circle node added add 12 o''clock
* add a marker for this roundabout if not already present
';


-- FUNCTION copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the road_graph_editing schema corresponding to the given editing session ID.';


-- FUNCTION create_queries_from_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.create_queries_from_editing_session(_editing_session_id integer) IS 'Build SQL queries to run from the editing_sessions.logged_ids column content for the given editing session';


-- FUNCTION drop_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.drop_editing_session(_editing_session_id integer) IS 'Delete the given editing session and truncate all the table of the road_graph_editing schema';


-- FUNCTION editing_survey()
COMMENT ON FUNCTION road_graph.editing_survey() IS 'Logs the modifications done inside the editing session schema tables.
It also check that the edited geometries are inside the editing session polygon.';


-- FUNCTION fix_graph_collapse_edge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_collapse_edge(_params jsonb) IS 'Action collapse_edge run by the function fix_graph_issue, to fix the issues of the check G08:
remove a short edge, by merging its two nodes.

The parameter _params must contain the ids of the edge, of the node to keep and of the node to remove:
{"edge_id": 12, "keep_node_id": 34, "remove_node_id": 56}.
The edge of the same road following the short edge at the node to remove is extended with the geometry
of the short edge up to the node to keep: the geometry of the road is kept. The start and end points
of the other edges of the node to remove, for example the edges of the other roads, are moved to the node to keep.
Then the edge and the node to remove are deleted. The only edge of a road is not removed.

It returns a JSON object with the keys status (success, skipped if the edge does not exist anymore
or its nodes have changed, or error) and message.';


-- FUNCTION fix_graph_cut_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_cut_edges(_params jsonb) IS 'Action cut_edges run by the function fix_graph_issue, to fix the issues of the check G01:
split the edges crossing each other at the given point.

The parameter _params must contain the crossing point as EWKT: {"point": "SRID=2154;POINT(x y)"}.
A node is created at this point, or the existing node less than 50 cm from it is used,
and the triggers split the edges passing there.

It returns a JSON object with the keys status (success, or skipped if no edge must be split at this point) and message.';


-- FUNCTION fix_graph_delete_node(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_delete_node(_params jsonb) IS 'Action delete_node run by the function fix_graph_issue, to fix the issues of the check G04:
delete a useless node.

The parameter _params must contain the id of the node: {"node_id": 12}.
A node without edge is deleted. A node between two edges of the same road is deleted
if one edge arrives at the node and the other one leaves it: the trigger before_node_delete merges the two edges
into the edge with the lowest id. If the merged edge is not between the start node of the first edge
and the end node of the second edge, for example because it has been split at a crossing with another edge,
an exception is raised: the function fix_graph_issue cancels the changes and returns an error.

It returns a JSON object with the keys status (success, skipped if the node does not exist anymore
or is not useless anymore, or error) and message.';


-- FUNCTION fix_graph_extend_edge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_extend_edge(_params jsonb) IS 'Action extend_edge run by the function fix_graph_issue, to fix the issues of the check G02:
extend an edge from its free end to the point to join on another edge.

The parameter _params must contain the edge id, the side of the free end (start or end),
the free end and the point to join as EWKT:
{"edge_id": 12, "side": "end", "point": "SRID=2154;POINT(x y)", "target": "SRID=2154;POINT(x y)"}.
When the point to join is the start or end point of the other edge, the free end is connected to its node,
and the node of the free end is deleted. Else, when the free end is less than 50 cm from the other edge,
the node of the free end is moved to the point to join. Else, the point to join is added at the free end of the edge,
and the triggers move the node of the free end to this point. In the two last cases, the triggers split the other edge there.
If the free end is not connected to another edge afterwards, an exception is raised:
the function fix_graph_issue cancels the changes and returns an error.

It returns a JSON object with the keys status (success, or skipped if the edge does not end at the given point anymore,
or is already connected there) and message.';


-- FUNCTION fix_graph_issue(_fix_action text, _params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_issue(_fix_action text, _params jsonb) IS 'Fix an issue of the road graph found by the function check_graph, with the given action and parameters.

The issues must be fixed in an editing session: the function only works in the editing schema,
with road_graph_editing.fix_graph_issue. It runs the function named fix_graph_ followed by the action name.

The issue may have been fixed by a previous action, for example when several issues concern the same edges:
the action then does nothing and returns the status skipped. If the action fails, its changes are cancelled
and the status error is returned with the error message.

It returns a JSON object with the keys status (success, skipped or error) and message.';


-- FUNCTION fix_graph_mark_as_bridge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_mark_as_bridge(_params jsonb) IS 'Action mark_as_bridge run by the function fix_graph_issue, to fix the issues of the check G01
when the edges must not be connected, for example on a bridge or in a tunnel.

It sets the field no_intersection_cutting to True for the edges passing by the crossing point:
the given edge if the parameter _params contains edge_id, or else all the edges of edge_ids:
{"point": "SRID=2154;POINT(x y)", "edge_ids": [12, 34], "edge_id": 12}.

It returns a JSON object with the keys status (success, or skipped if the edges are already marked) and message.';


-- FUNCTION fix_graph_merge_nodes(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_merge_nodes(_params jsonb) IS 'Action merge_nodes run by the function fix_graph_issue, to fix the issues of the check G03:
merge two nodes close to each other.

The parameter _params must contain the ids of the node to keep and of the node to remove:
{"keep_node_id": 12, "remove_node_id": 34}.
The start and end points of the edges of the node to remove are moved to the node to keep,
then the node to remove is deleted.

It returns a JSON object with the keys status (success, skipped if the node to remove does not exist anymore,
or error if the nodes are linked by an edge) and message.';


-- FUNCTION fix_graph_reverse_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_reverse_edges(_params jsonb) IS 'Action reverse_edges run by the function fix_graph_issue, to fix the issues of the check G05:
reverse the edges in the opposite direction of their road.

The parameter _params must contain the ids of the edges: {"edge_ids": [12, 34]}.
Only the edges still found by the check G05 are reversed, one by one: the triggers swap their start and end nodes,
and calculate again their previous and next edges and the references of their road.

It returns a JSON object with the keys status (success, skipped if the edges are not in the opposite direction
of their road anymore, or error) and message.';


-- FUNCTION get_current_setting(setting_name text, default_value text)
COMMENT ON FUNCTION road_graph.get_current_setting(setting_name text, default_value text) IS 'Get a PostgreSQL current setting, with a default value if the setting is not set or is invalid.
The function is used to have a single point of maintenance for getting settings.';


-- FUNCTION get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real)
COMMENT ON FUNCTION road_graph.get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real) IS 'Returns a JSON object with the given references and the MULTILINESTRING downstream road from given references to the road end.';


-- FUNCTION get_edge_references(_edge_id integer, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_edge_references(_edge_id integer, _use_cache boolean) IS 'Return the references of the given edge start and end point.
Could be used to UPDATE the edges of a road.
By default does not use road object cache store in a temporary table.
This behaviour can be overriden by setting _use_cache TO True;
';


-- FUNCTION get_editing_session_impacts(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.get_editing_session_impacts(_editing_session_id integer) IS 'Get the impacts of the given editing session on the roads, to update only the managed objects which may be impacted.
It must be run before the merge of the editing session, since it compares the production and editing data.
It returns a JSON array with one object per impacted road:
* road_code,
* whole_road: true if all the objects of the road must be updated (references not calculated for the modified edges, for example for the roundabouts),
* geometry_changed: false if the edges have only been split, or if only their attributes have changed,
* start_marker_code & start_abscissa: the references of the first modified point,
* end_marker_code: the last marker of the modified edges,
* near_geom: the modified edges geometries, before and after the modifications, as hexadecimal EWKB,
* marker_ranges: for each modified marker, the code of the previous marker and its code.
The function get_managed_objects_impacts_filter uses this JSON to filter the managed objects to update.';


-- FUNCTION get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text) IS 'Get the geometry of a managed object from its references, with the functions get_road_point_from_reference for a point (_end_marker_code is NULL)
or get_road_substring_from_references for a line.
It returns NULL with a warning instead of raising an exception if the geometry cannot be calculated,
for example if the end references are before the start references.';


-- FUNCTION get_managed_objects_impacts_filter(_geometry_type text, _table_cols text[], _geometry_column text)
COMMENT ON FUNCTION road_graph.get_managed_objects_impacts_filter(_geometry_type text, _table_cols text[], _geometry_column text) IS 'Build the SQL filter of the managed objects which may be impacted by the modifications of the road graph.
The managed table must have the alias mo, and the impacts returned by the function get_editing_session_impacts must be given as the first parameter $1 of the query.';


-- FUNCTION get_merged_geom_from_table_and_ids(_schema_name text, _table_name text, _ids integer[])
COMMENT ON FUNCTION road_graph.get_merged_geom_from_table_and_ids(_schema_name text, _table_name text, _ids integer[]) IS 'Compute the union geometry of all the features from a table and given feature ids';


-- FUNCTION get_or_create_edge_node(_point geometry, _create boolean)
COMMENT ON FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) IS 'Get the node of the given start or end point of an edge: the closest node less than 0.5 m from it,
or the node with the lowest id if several nodes are at the same distance.

If there is no node and _create is True, the node is created (its id can be NULL if a node already exists at the same place).
Used by the trigger function before_edge_insert_or_update.';


-- FUNCTION get_ordered_edges(_road_code text, _initial_id integer, _direction text)
COMMENT ON FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer, _direction text) IS 'Get the list of edge id with order for a given road, edge and the direction (downtream or upstream). It always includes the given edge';


-- FUNCTION get_reference_from_point(_point geometry, _road_code text, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text, _use_cache boolean) IS 'Calculate the references for the given point. The second parameter _road_code allows to narrow the search to the specified road.
Since this method is heavily used and the calculation is costly, we can pass a third parameter _use_cache which allows to use a pre-generated cache (to be build before using this function).';


-- FUNCTION get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the geometry of the corresponding point';


-- FUNCTION get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean) IS 'Get the closest upstream marker for the given road from a given point.
This function can use roads cached objects generated beforehand via function build_road_cached_objects.
Or use a full SQL query with no use of temporary tables, depending of the paramter _use_cache

Illustration
|m0----m1----|  |-m2-m2b----m3----|    p3   m4  |--------m5---|
                 p0     p1                         p2
p0 -> marker is m1
p1 -> marker is m2b (virtual marker with a non-null abscissa)
p2 -> marker is m3
p3 -> marker is m4, since it is the start marker of the first next edge and p3 is not on a real previous edge.

The function also returns:
* the simple linestring (no gaps) made by merging all edges linestrings from the marker to the point
* the simple linestring (no gaps) made by merging all edges linestrings from the start to the point
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
* if the found marker is the start marker of the next edge (for p3-like cases when the point is between two edges)
';


-- FUNCTION get_road_sections_from_snapped_line(_line geometry, _tolerance real)
COMMENT ON FUNCTION road_graph.get_road_sections_from_snapped_line(_line geometry, _tolerance real) IS 'Return the ordered list of the roads followed by the given line, with the start and end references (marker, abscissa and cumulative) for each road, the direction (downstream or upstream) and the line portion on this road.
The line must follow the road graph edges, for example the result of the function get_snapped_line_from_track. The parameter _tolerance (default 1m) is the maximum distance between the line and the edges.
A line without SRID is considered to be in the road graph projection, and other lines are reprojected.
Example of output (the geom column is not shown):
 road_order | road_code     | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative | direction  | length
------------+---------------+--------------+----------------+------------------+------------+--------------+----------------+------------+--------
          1 | D40           |           14 |         905.27 |          16361.8 |         15 |       396.15 |        16761.4 | downstream |  399.6
          2 | GIR_D40_D16-1 |            0 |              0 |                0 |          0 |        39.17 |          39.17 | downstream |  39.17
          3 | D16           |            0 |         716.82 |           716.82 |          0 |         1.62 |           1.62 | upstream   | 715.19
';


-- FUNCTION get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text, _from_first_edge_start boolean, _to_last_edge_end boolean)
COMMENT ON FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text, _from_first_edge_start boolean, _to_last_edge_end boolean) IS 'Returns a JSON object with the given references and the geometry of the built linestring. The produced multilinestring geometry has been reordered based on the graph if it contains more than one part.
The parameters _from_first_edge_start & _to_last_edge_end allows to respectively add the edge under the result linestring start point & the edge under the result linestring end point.';


-- FUNCTION get_snapped_line_from_track(_track geometry, _densify_distance real, _max_distance real, _road_change_penalty real)
COMMENT ON FUNCTION road_graph.get_snapped_line_from_track(_track geometry, _densify_distance real, _max_distance real, _road_change_penalty real) IS 'Build a multilinestring snapped on the road graph edges from a GPS track (for example from a GPX file) which can be imprecise.
The track is densified with a point every _densify_distance meters (default 2m), and each point is assigned to one of the roads closer than _max_distance (default 25m).
To avoid zigzags between two close roads, the chosen sequence of roads minimizes the sum of the distances between the points and their roads, plus a penalty _road_change_penalty (default 100, expressed as a surface in m² between the track and the roads) each time the road changes.
The returned geometry follows the edges geometries, in the direction of the track. Gaps between two unconnected edges are filled with the shortest path of maximum 5 edges. If no path is found, a new part is created in the multilinestring.
A track without SRID is considered to be in the road graph projection, and other tracks are reprojected. The result is returned in the road graph projection.
The result can be passed to the function get_road_sections_from_snapped_line to get the references of the crossed roads.';


-- FUNCTION get_spatial_road(_road_code text)
COMMENT ON FUNCTION road_graph.get_spatial_road(_road_code text) IS 'Build the road geometry for the given road id. It can then be used with ST_LocateAlong.';


-- FUNCTION get_updated_roads_from_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.get_updated_roads_from_editing_session(_editing_session_id integer) IS 'Get distinct road_code values from the objects concerned by editing_sessions.logged_ids column content for the given editing session';


-- FUNCTION get_upper_roundabout_node_to_delete(_road_code text)
COMMENT ON FUNCTION road_graph.get_upper_roundabout_node_to_delete(_road_code text) IS 'Get the roundabout node at the top of the circle. It only exists because QGIS creates a circle with a point at the top.
We need to delete this node and merge edges around it.
This function does nothing if no node is found at the exact top position of the circle.
';


-- FUNCTION import_data_from_template_tables(_source_schema text, _source_edges_table text, _source_markers_table text)
COMMENT ON FUNCTION road_graph.import_data_from_template_tables(_source_schema text, _source_edges_table text, _source_markers_table text) IS 'Import data from the given schema and edges & markers source tables';


-- FUNCTION merge_edges(id_edge_a integer, id_edge_b integer)
COMMENT ON FUNCTION road_graph.merge_edges(id_edge_a integer, id_edge_b integer) IS 'Merge two edges by their geometry.
The merged edge is the one with the lowest id. The other edge is deleted.
The node between the two edges is not deleted but its id is stored
in the session variable ''road.graph.merge.edges.useless.node'' for further use if needed.
';


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';


-- FUNCTION merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer)
COMMENT ON FUNCTION road_graph.merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer) IS 'Merge two linestring features of the same table.

The feature A, chosen first by the user, is kept with its field values
and gets the merged geometry, in the road direction. The feature B is deleted.

If the table has the references fields, the references of the feature A
(and its cumulative distances if the fields exist) are calculated from the merged geometry, in the road direction.

The lines connected to the features are not modified by the trigger trg_update_managed_object_linked_geometries.

It returns a JSON object with the keys status (success or error) and message.';


-- FUNCTION reorder_multilinestring_parts(_multilinestring geometry, _road_code text)
COMMENT ON FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text) IS 'Reorder the parts of the given road MULTILINESTRING based on the graph.
For each part, the references of the start & end points is calculated, which helps to reorder the parts.
We also reverse the geometry if needed (since the ST_OffsetCurve sometimes does not respect the node order)
';


-- FUNCTION split_edge_by_node(edge_record record, node_record record, minimum_distance_from_linestring real, maximum_distance_from_start_end_points real)
COMMENT ON FUNCTION road_graph.split_edge_by_node(edge_record record, node_record record, minimum_distance_from_linestring real, maximum_distance_from_start_end_points real) IS 'Split an edge by a node. The node must be close enough to the edge and not too close from the edge start or end point.
The function returns the id of the new created edge or null if the edge has not been split.';


-- FUNCTION split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry)
COMMENT ON FUNCTION road_graph.split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry) IS 'Splits a table linestring feature by a given point.

The two new geometries follow the road direction, even if the feature is digitized in the opposite direction,
like the geometries calculated from the references.
The feature keeps the first part in the road direction, and a new feature is created with the second part.

If the table has the fields start_marker_code, start_abscissa, end_marker_code and end_abscissa,
the references of the two features are calculated in the road direction (start before end):
the references of the cut point become the end references of the feature and the start references of the new feature.
The fields start_cumulative and end_cumulative are also set if they exist.

The lines connected to the feature are not modified by the trigger trg_update_managed_object_linked_geometries.';


-- FUNCTION toggle_foreign_key_constraints(_toggle boolean)
COMMENT ON FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) IS 'Deactivate or reactivate the foreign key constraints to ease
the merging road_graph_editing data into road_graph schema.
It is also used when importing data from templates.

Under the hood, it drops or drops & creates the constraint
depending on the value of the _toggle parameter
';


-- FUNCTION update_edge_references(_road_code text, _edge_ids integer[])
COMMENT ON FUNCTION road_graph.update_edge_references(_road_code text, _edge_ids integer[]) IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges
';


-- FUNCTION update_managed_object_linked_geometries()
COMMENT ON FUNCTION road_graph.update_managed_object_linked_geometries() IS 'Trigger function updating the lines of a managed object table
which were connected to the start or the end of an updated line.

The linked lines are the other lines of the same road whose start or end point touched (5 cm tolerance)
the start or end point of the line before its modification:

* an upstream linked line gets the new start references of the line as end references,
* a downstream linked line gets the new end references of the line as start references,
* the cumulative distances are also updated if the fields exist.

Their geometry is built again with the function get_road_substring_from_references,
keeping their offset and side: they are shortened or extended to stay connected.
A linked line is not modified if its new references are not ordered (the line has been extended over it).

It must be used in an AFTER UPDATE OF geom trigger, so that the references of the updated line
are already calculated, for example by the trigger trg_calculate_managed_object_references.
The function road_graph.add_managed_objects_triggers creates this trigger, named trg_update_managed_object_linked_geometries.

The table must be registered in road_graph.managed_objects with a line geometry type, and have the fields
road_code, start_marker_code, start_abscissa, end_marker_code and end_abscissa.

The trigger is disabled by the session variables road.graph.disable.trigger
or road.graph.disable.linked.geometries set to 1.';


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb) IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table.
The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.';


-- FUNCTION update_road_edges_neighbours(_road_code text, _start_node integer)
COMMENT ON FUNCTION road_graph.update_road_edges_neighbours(_road_code text, _start_node integer) IS 'Try to calculate the previous_edge_id and next_edge_id of the given road edges based on the edge proximity
and an initial node given
';


-- FUNCTION update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb) IS 'Update the given table geometries based on the references. This function needs the table to be listed in the table road_graph.managed_objects.
The objects whose geometry cannot be calculated are not updated, and a warning is raised.
The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.';


-- FUNCTION update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean, _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean, _impacts jsonb) IS 'Update the given table references based on the geometries. This function needs the table to be listed in the table road_graph.managed_objects.
The given columns must exists:
* for points: road_code, marker_code, abscissa. Optional columns: offset & side,
* road_code, start_marker_code, start_abscissa, end_marker_code, end_abscissa. Optional columns: start_cumulative, end_cumulative, offset & side

The parameter _update_offset_and_side allows to not update the offset and side columns of the target table.
It is useful when used before updating the table geometries from the references
(to keep the object in the same start and end places but adapt the geometry)

The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.
';


-- edges
COMMENT ON TABLE road_graph.edges IS 'Road graph edges';


-- edges.id
COMMENT ON COLUMN road_graph.edges.id IS 'Unique integer ID (automatic)';


-- edges.road_code
COMMENT ON COLUMN road_graph.edges.road_code IS 'The code of the parent road. Used to group the edges together. Ex: RD230';


-- edges.start_node
COMMENT ON COLUMN road_graph.edges.start_node IS 'The start node (also called source node or upstream node)';


-- edges.end_node
COMMENT ON COLUMN road_graph.edges.end_node IS 'The end node (also called target node or downstream node)';


-- edges.no_intersection_cutting
COMMENT ON COLUMN road_graph.edges.no_intersection_cutting IS 'Prevents the trigger functions from automatically cutting the edges having this value set to True.
Useful for access roads, edges located under a bridge, etc.
No node is created at the intersection with other edges, and the existing nodes under the edge do not cut it.
Exception: an edge is cut if the start or end point of a created or moved edge is located on it,
for example a new access road arriving in the middle of a highway edge.';


-- edges.id_source
COMMENT ON COLUMN road_graph.edges.id_source IS 'Original ID of the source data (used after an import, for example to check references between original data and the imported data in road_graph';


-- editing_sessions.id
COMMENT ON COLUMN road_graph.editing_sessions.id IS 'Unique ID';


-- editing_sessions.label
COMMENT ON COLUMN road_graph.editing_sessions.label IS 'Editing session label. Keep it short';


-- editing_sessions.author
COMMENT ON COLUMN road_graph.editing_sessions.author IS 'Author of the editing session';


-- editing_sessions.created_at
COMMENT ON COLUMN road_graph.editing_sessions.created_at IS 'Creation timestamp';


-- editing_sessions.updated_at
COMMENT ON COLUMN road_graph.editing_sessions.updated_at IS 'Update timestamp';


-- editing_sessions.description
COMMENT ON COLUMN road_graph.editing_sessions.description IS 'Description of the session';


-- editing_sessions.status
COMMENT ON COLUMN road_graph.editing_sessions.status IS 'Current status : created, cloned, edited, merged';


-- editing_sessions.unique_code
COMMENT ON COLUMN road_graph.editing_sessions.unique_code IS 'Automatically generated code with timestamp like editing_session_2025_09_26_08_37_24';


-- editing_sessions.cloned_ids
COMMENT ON COLUMN road_graph.editing_sessions.cloned_ids IS 'Object containing the list of unique ids for each cloned data (roads, edges, nodes, etc.)';


-- glossary_graph_check
COMMENT ON TABLE road_graph.glossary_graph_check IS 'Checks of the road graph run by the function check_graph. The rows are added by the plugin migrations: only the columns default_options and is_active should be modified.';


-- glossary_graph_check.code
COMMENT ON COLUMN road_graph.glossary_graph_check.code IS 'Code of the check, e.g. G01. The checks are numbered in the recommended order of the fixes.';


-- glossary_graph_check.title
COMMENT ON COLUMN road_graph.glossary_graph_check.title IS 'Title of the check';


-- glossary_graph_check.severity
COMMENT ON COLUMN road_graph.glossary_graph_check.severity IS 'Severity of the issues found by the check: error or warning';


-- glossary_graph_check.function_name
COMMENT ON COLUMN road_graph.glossary_graph_check.function_name IS 'Name of the function running the check, in the same schema as the function check_graph';


-- glossary_graph_check.fix_actions
COMMENT ON COLUMN road_graph.glossary_graph_check.fix_actions IS 'Actions which can fix the issues found by the check, with the function fix_graph_issue';


-- glossary_graph_check.default_options
COMMENT ON COLUMN road_graph.glossary_graph_check.default_options IS 'Default options of the check, for example its tolerance. They can be modified by the administrator, and overridden by the options given to the function check_graph.';


-- glossary_graph_check.is_active
COMMENT ON COLUMN road_graph.glossary_graph_check.is_active IS 'If False, the check is not run by the function check_graph, unless it is explicitly asked';


-- glossary_graph_check.description
COMMENT ON COLUMN road_graph.glossary_graph_check.description IS 'Description of the check and of its fix actions';


-- glossary_road_class
COMMENT ON TABLE road_graph.glossary_road_class IS 'Glossary for the column road_scale of the table roads';


-- managed_objects
COMMENT ON TABLE road_graph.managed_objects IS 'Table listing all managed objects in the road graph system';


-- managed_objects.schema_name
COMMENT ON COLUMN road_graph.managed_objects.schema_name IS 'The schema name of the managed object table';


-- managed_objects.table_name
COMMENT ON COLUMN road_graph.managed_objects.table_name IS 'The table name of the managed object table';


-- managed_objects.geometry_type
COMMENT ON COLUMN road_graph.managed_objects.geometry_type IS 'The geometry type of the managed object table. It can be POINT or LINESTRING or MULTILINESTRING.';


-- managed_objects.update_policy_on_graph_change
COMMENT ON COLUMN road_graph.managed_objects.update_policy_on_graph_change IS 'The update policy to apply on the managed objects when the road graph changes.
It can be "geometry" to update only the geometries based on the references,
"references" to update only the references based on the geometries
or "none" to not update anything.';


-- managed_objects.last_update
COMMENT ON COLUMN road_graph.managed_objects.last_update IS 'The date and time of the last update of table objects based on the road graph.';


-- managed_objects.last_updated_objects_ids
COMMENT ON COLUMN road_graph.managed_objects.last_updated_objects_ids IS 'The list of the last updated objects ids based on the road graph.';


-- managed_objects.add_trigger_on_geom_change
COMMENT ON COLUMN road_graph.managed_objects.add_trigger_on_geom_change IS 'If True, the function road_graph.add_managed_objects_triggers will create the trigger
trg_calculate_managed_object_references on this table, which calculates the references
of the features on insert or on geometry update, and for the line tables the trigger
trg_update_managed_object_linked_geometries, which updates the lines connected to an updated line.';


-- markers
COMMENT ON TABLE road_graph.markers IS 'Road distance markers';


-- markers.id
COMMENT ON COLUMN road_graph.markers.id IS 'Unique integer ID (automatic)';


-- markers.road_code
COMMENT ON COLUMN road_graph.markers.road_code IS 'Code of the parent road';


-- markers.code
COMMENT ON COLUMN road_graph.markers.code IS 'Code of the marker. Should be unique for a given road code';


-- markers.id_source
COMMENT ON COLUMN road_graph.markers.id_source IS 'Original ID of the source data (used after an import, for example to check references between original data and the imported data in road_graph';


-- metadata
COMMENT ON TABLE road_graph.metadata IS 'Metadata of the structure : version and date. Usefull for database structure and glossary data migrations between versions';


-- nodes
COMMENT ON TABLE road_graph.nodes IS 'Road graph nodes';


-- nodes.id
COMMENT ON COLUMN road_graph.nodes.id IS 'Unique integer ID (automatic)';


-- roads
COMMENT ON TABLE road_graph.roads IS 'Roads. This is a non-spatial table listing all unique roads codes. It is a parent of the table edges.';


-- roads.id
COMMENT ON COLUMN road_graph.roads.id IS 'Unique integer ID (automatic)';


-- roads.road_type
COMMENT ON COLUMN road_graph.roads.road_type IS 'Type of the road : road, roundabout. It is used by the trigger functions: glossary must be respected';


-- VIEW v_managed_objects
COMMENT ON VIEW road_graph.v_managed_objects IS 'View allowing to see the merged geometries of each managed table for the last editing session merge. Useful to have a quick look at the edited objects.';


-- MATERIALIZED VIEW vm_managed_objects
COMMENT ON MATERIALIZED VIEW road_graph.vm_managed_objects IS 'Materialized view based on the view v_managed_objects, showing the merged geometries of each managed table
for the last editing session merge. It is refreshed at the end of the function merge_editing_session_data.';


--
-- PostgreSQL database dump complete
--



