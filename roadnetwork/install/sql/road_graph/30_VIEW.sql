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

-- v_managed_objects
CREATE VIEW road_graph.v_managed_objects AS
 SELECT m.id,
    m.schema_name,
    m.table_name,
    m.geometry_type,
    m.update_policy_on_graph_change,
    m.last_update,
    m.last_updated_objects_ids,
    road_graph.get_merged_geom_from_table_and_ids(m.schema_name, m.table_name, m.last_updated_objects_ids) AS geom
   FROM road_graph.managed_objects m;


-- VIEW v_managed_objects
COMMENT ON VIEW road_graph.v_managed_objects IS 'View allowing to see the merged geometries of each managed table for the last editing session merge. Useful to have a quick look at the edited objects.';


-- v_road_without_zero_marker
CREATE VIEW road_graph.v_road_without_zero_marker AS
 SELECT DISTINCT e.road_code
   FROM (road_graph.edges e
     LEFT JOIN road_graph.markers m ON (((m.road_code = e.road_code) AND (m.code = 0))))
  WHERE (m.id IS NULL);


--
-- PostgreSQL database dump complete
--



