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
-- Data for Name: glossary_test_category; Type: TABLE DATA; Schema: road_graph; Owner: -
--

INSERT INTO road_graph.glossary_test_category (id, code, label) VALUES (1, 'A', 'Category A');
INSERT INTO road_graph.glossary_test_category (id, code, label) VALUES (2, 'B', 'Category B');


--
-- Name: glossary_test_category_id_seq; Type: SEQUENCE SET; Schema: road_graph; Owner: -
--

SELECT pg_catalog.setval('road_graph.glossary_test_category_id_seq', 2, true);


--
-- PostgreSQL database dump complete
--



