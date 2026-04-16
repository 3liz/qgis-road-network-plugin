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

-- edges_end_node_idx
CREATE INDEX edges_end_node_idx ON road_graph.edges USING btree (end_node);


-- edges_geom_idx
CREATE INDEX edges_geom_idx ON road_graph.edges USING gist (geom);


-- edges_road_code_idx
CREATE INDEX edges_road_code_idx ON road_graph.edges USING btree (road_code);


-- edges_st_endpoint_idx
CREATE INDEX edges_st_endpoint_idx ON road_graph.edges USING gist (public.st_endpoint(geom));


-- edges_st_startpoint_idx
CREATE INDEX edges_st_startpoint_idx ON road_graph.edges USING gist (public.st_startpoint(geom));


-- edges_start_node_idx
CREATE INDEX edges_start_node_idx ON road_graph.edges USING btree (start_node);


-- editing_sessions_geom_idx
CREATE INDEX editing_sessions_geom_idx ON road_graph.editing_sessions USING gist (geom);


-- managed_objects_geometry_type_idx
CREATE INDEX managed_objects_geometry_type_idx ON road_graph.managed_objects USING btree (geometry_type);


-- managed_objects_object_type_idx
CREATE INDEX managed_objects_object_type_idx ON road_graph.managed_objects USING btree (object_type);


-- markers_geom_idx
CREATE INDEX markers_geom_idx ON road_graph.markers USING gist (geom);


-- nodes_geom_idx
CREATE INDEX nodes_geom_idx ON road_graph.nodes USING gist (geom);


-- roads_road_code_idx
CREATE INDEX roads_road_code_idx ON road_graph.roads USING btree (road_code);


--
-- PostgreSQL database dump complete
--



