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

--
-- Data for Name: glossary_graph_check; Type: TABLE DATA; Schema: road_graph; Owner: -
--

INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G01', 'Crossing edges without node', 'error', 'check_graph_crossing_edges', '{cut_edges,mark_as_bridge}', '{}', true, 'Edges crossing or touching each other without being split by a node. The edges with no_intersection_cutting set to True are ignored. Fix: cut_edges creates a node at the point and splits the edges, mark_as_bridge sets no_intersection_cutting to True for the edges which must not be connected (bridge, tunnel). The edges touching each other are fixed with cut_edges. For the edges crossing each other, the action must be chosen for each issue, unless the option crossing_fix_action is set.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G02', 'Edge ending near another edge without being connected', 'error', 'check_graph_t_junction_gaps', '{extend_edge}', '{"tolerance": 2}', true, 'Free end of an edge (no other edge connected to it) less than the tolerance from another edge, for example a road stopping a few decimeters before the road it arrives on (T junction). Option: tolerance, in meters. Fix: extend_edge extends the edge to the closest point of the other edge, or to its start or end point if it is less than 50 cm from it, and the other edge is split there.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G03', 'Nodes close to each other', 'error', 'check_graph_close_nodes', '{merge_nodes}', '{"tolerance": 1}', true, 'Nodes less than the tolerance from each other, which should be the same node. The nodes linked by an edge are ignored. Option: tolerance, in meters. Fix: merge_nodes moves the edges of the other node to the node with the most edges, or else to the node with the lowest id, then deletes the other node.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G04', 'Useless node', 'warning', 'check_graph_useless_nodes', '{delete_node}', '{}', true, 'Node without edge, or node between two edges of the same road which can be merged: one edge arrives at the node and the other one leaves it, with the same value of no_intersection_cutting. Fix: delete_node deletes the node, and the triggers merge the two edges into the edge with the lowest id.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G05', 'Edges in the opposite direction of the road', 'error', 'check_graph_reversed_edges', '{reverse_edges}', '{"marker_tolerance": 10}', true, 'Edges in the opposite direction of their road. The edges of a road following each other are grouped in sequences, whose direction is given by the order of the markers along them (option marker_tolerance, in meters), or else by the direction of most of their length. Fix: reverse_edges reverses the geometry of the edges, and the triggers swap their start and end nodes.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G06', 'Road with branches', 'error', 'check_graph_road_branches', '{}', '{}', true, 'Node with more than two edges of the same road, for example the ramps or the carriageways of a road using the code of the road. The roundabouts are ignored. The branches are the edges which are not the most aligned edges arriving at the node and leaving it. Fix: manual, for example by giving another road code to the edges of the branches.');
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, is_active, description) VALUES ('G07', 'Parallel edges of the same road', 'warning', 'check_graph_parallel_edges', '{}', '{"tolerance": 30, "min_length": 10}', true, 'Edges of the same road, not connected by a node, running along each other less than the tolerance from each other along at least the minimum length, for example the two carriageways of a road using the same road code. The edges which only approach each other, for example on both sides of a roundabout, and the roundabouts are ignored. Options: tolerance and min_length, in meters. Fix: manual, for example by giving another road code to one of the carriageways.');


--
-- Data for Name: glossary_road_class; Type: TABLE DATA; Schema: road_graph; Owner: -
--

INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (1, 'Intercommunale', 'Intercommunale');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (2, 'Autoroute', 'Autoroute');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (3, 'Nationale', 'Nationale');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (4, 'Communale', 'Communale');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (5, 'Autre', 'Autre');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (6, 'Voie privée', 'Voie privée');
INSERT INTO road_graph.glossary_road_class (id, code, label) VALUES (7, 'Départementale', 'Départementale');


--
-- Name: glossary_road_class_id_seq; Type: SEQUENCE SET; Schema: road_graph; Owner: -
--

SELECT pg_catalog.setval('road_graph.glossary_road_class_id_seq', 7, true);


--
-- PostgreSQL database dump complete
--



