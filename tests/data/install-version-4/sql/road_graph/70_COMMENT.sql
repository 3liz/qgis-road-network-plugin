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


-- FUNCTION after_edge_delete()
COMMENT ON FUNCTION road_graph.after_edge_delete() IS 'Lors de la suppression d''un tronçon, on supprime les Nodes qui étaient rattachés aux sommets amont et aval du tronçon,
uniquement si ces derniers ne sont plus rattachés à aucun autre tronçon.
On fusionne aussi les tronçons collés plus liés par les possibles noeuds supprimés
';


-- FUNCTION after_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.after_edge_insert_or_update() IS 'Multiples opérations lancées suite à la modification d''un troncon.
Déplacement du noeud initial et terminal liés si besoin.
Suppression des noeuds orphelins si besoin.
Création des noeuds non existants à l''intersection avec les autres edges';


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
COMMENT ON FUNCTION road_graph.before_edge_insert_or_update() IS ' ';


-- FUNCTION before_editing_sessions_update()
COMMENT ON FUNCTION road_graph.before_editing_sessions_update() IS 'Prevent from updating an editing session geometry if there is data inside the editing_session schema';


-- FUNCTION clean_digitized_roundabout(_road_code text)
COMMENT ON FUNCTION road_graph.clean_digitized_roundabout(_road_code text) IS 'Clean a roundabout digitized by QGIS circle tool:
* delete the edges inside the roundabout
* remove the circle node added add 12 o''clock
* add a marker for this roundabout if not already present
';


-- FUNCTION copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the editing_session schema corresponding to the given editing session ID.';


-- FUNCTION create_queries_from_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.create_queries_from_editing_session(_editing_session_id integer) IS 'Build SQL queries to run from the editing_sessions.logged_ids column content for the given editing session';


-- FUNCTION editing_survey()
COMMENT ON FUNCTION road_graph.editing_survey() IS 'Logs the modifications done inside the editing session schema tables.
It also check that the edited geometries are inside the editing session polygon.';


-- FUNCTION get_edge_references(edge_id integer)
COMMENT ON FUNCTION road_graph.get_edge_references(edge_id integer) IS 'Return the references of the given edge start and end point.
Could be used to UPDATE the edges of a road.
';


-- FUNCTION get_ordered_edges(_road_code text, _initial_id integer, _direction text)
COMMENT ON FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer, _direction text) IS 'Get the list of edge id with order for a given road, edge and the direction (downtream or upstream). It always includes the given edge';


-- FUNCTION get_road_previous_marker_from_point(_road_code text, _point geometry)
COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry) IS 'Get the closest upstream marker for the given road from a given point.
|m0----m1----|  |----m2----m3----|   |--------m4---|
                 p0    p1               p2
p0 -> marker is m1
p1 -> marker is m2
p2 -> marker is m3
The function also returns 
* the simple linestring (no gaps) made by merging all edges linestrings from the marker to the point
* the simple linestring (no gaps) made by merging all edges linestrings from the start to the point
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
';


-- FUNCTION get_spatial_road(_road_code text)
COMMENT ON FUNCTION road_graph.get_spatial_road(_road_code text) IS 'Build the road geometry for the given road id. It can then be used with ST_LocateAlong.';


-- FUNCTION get_upper_roundabout_node_to_delete(_road_code text)
COMMENT ON FUNCTION road_graph.get_upper_roundabout_node_to_delete(_road_code text) IS 'Get the roundabout node at the top of the circle. It only exists because QGIS creates a circle with a point at the top.
We need to delete this node and merge edges around it.
This function does nothing if no node is found at the exact top position of the circle.
';


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';


-- FUNCTION toggle_foreign_key_constraints(_toggle boolean)
COMMENT ON FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) IS 'Deactivate foreign key constraints to ease the merging editing_session data into road_graph schema';


-- FUNCTION update_edge_references(_road_code text, _edge_ids integer[])
COMMENT ON FUNCTION road_graph.update_edge_references(_road_code text, _edge_ids integer[]) IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references
';


-- FUNCTION update_road_edges_neighbours(_road_code text, _start_node integer)
COMMENT ON FUNCTION road_graph.update_road_edges_neighbours(_road_code text, _start_node integer) IS 'Try to calculate the previous_edge_id and next_edge_id of the given road edges based on the edge proximity
and an initial node given
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


-- glossary_road_class
COMMENT ON TABLE road_graph.glossary_road_class IS 'Glossary for the column road_scale of the table roads';


-- markers
COMMENT ON TABLE road_graph.markers IS 'Road distance markers';


-- markers.id
COMMENT ON COLUMN road_graph.markers.id IS 'Unique integer ID (automatic)';


-- markers.road_code
COMMENT ON COLUMN road_graph.markers.road_code IS 'Code of the parent road';


-- markers.code
COMMENT ON COLUMN road_graph.markers.code IS 'Code of the marker. Should be unique for a given road code';


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


--
-- PostgreSQL database dump complete
--



