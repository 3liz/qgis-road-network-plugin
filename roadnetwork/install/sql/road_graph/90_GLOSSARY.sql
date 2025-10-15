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
-- Name: glossary_road_scale_id_seq; Type: SEQUENCE SET; Schema: road_graph; Owner: -
--

SELECT pg_catalog.setval('road_graph.glossary_road_scale_id_seq', 7, true);


--
-- PostgreSQL database dump complete
--



