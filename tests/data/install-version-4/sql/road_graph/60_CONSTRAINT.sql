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

SET default_tablespace = '';

-- edges edges_pkey
ALTER TABLE ONLY road_graph.edges
    ADD CONSTRAINT edges_pkey PRIMARY KEY (id);


-- editing_sessions editing_sessions_pkey
ALTER TABLE ONLY road_graph.editing_sessions
    ADD CONSTRAINT editing_sessions_pkey PRIMARY KEY (id);


-- glossary_road_class glossary_road_scale_code_key
ALTER TABLE ONLY road_graph.glossary_road_class
    ADD CONSTRAINT glossary_road_scale_code_key UNIQUE (code);


-- glossary_road_class glossary_road_scale_label_key
ALTER TABLE ONLY road_graph.glossary_road_class
    ADD CONSTRAINT glossary_road_scale_label_key UNIQUE (label);


-- glossary_road_class glossary_road_scale_pkey
ALTER TABLE ONLY road_graph.glossary_road_class
    ADD CONSTRAINT glossary_road_scale_pkey PRIMARY KEY (id);


-- markers markers_pkey
ALTER TABLE ONLY road_graph.markers
    ADD CONSTRAINT markers_pkey PRIMARY KEY (id);


-- markers markers_road_code_code_abscissa_key
ALTER TABLE ONLY road_graph.markers
    ADD CONSTRAINT markers_road_code_code_abscissa_key UNIQUE (road_code, code, abscissa);


-- nodes nodes_geom_key
ALTER TABLE ONLY road_graph.nodes
    ADD CONSTRAINT nodes_geom_key UNIQUE (geom);


-- nodes nodes_pkey
ALTER TABLE ONLY road_graph.nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (id);


-- roads roads_pkey
ALTER TABLE ONLY road_graph.roads
    ADD CONSTRAINT roads_pkey PRIMARY KEY (id);


-- roads roads_road_code_key
ALTER TABLE ONLY road_graph.roads
    ADD CONSTRAINT roads_road_code_key UNIQUE (road_code);


-- edges edges_end_node_fkey
ALTER TABLE ONLY road_graph.edges
    ADD CONSTRAINT edges_end_node_fkey FOREIGN KEY (end_node) REFERENCES road_graph.nodes(id) ON UPDATE CASCADE ON DELETE RESTRICT;


-- edges edges_road_code_fkey
ALTER TABLE ONLY road_graph.edges
    ADD CONSTRAINT edges_road_code_fkey FOREIGN KEY (road_code) REFERENCES road_graph.roads(road_code) ON UPDATE CASCADE ON DELETE RESTRICT;


-- edges edges_start_node_fkey
ALTER TABLE ONLY road_graph.edges
    ADD CONSTRAINT edges_start_node_fkey FOREIGN KEY (start_node) REFERENCES road_graph.nodes(id) ON UPDATE CASCADE ON DELETE RESTRICT;


-- markers markers_road_code_fkey
ALTER TABLE ONLY road_graph.markers
    ADD CONSTRAINT markers_road_code_fkey FOREIGN KEY (road_code) REFERENCES road_graph.roads(road_code) ON UPDATE CASCADE ON DELETE RESTRICT;


-- roads roads_road_scale_fkey
ALTER TABLE ONLY road_graph.roads
    ADD CONSTRAINT roads_road_scale_fkey FOREIGN KEY (road_class) REFERENCES road_graph.glossary_road_class(code) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--



