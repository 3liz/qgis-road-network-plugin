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

-- edges trg_aa_before_geometry_insert_or_update
CREATE TRIGGER trg_aa_before_geometry_insert_or_update BEFORE INSERT OR UPDATE OF geom ON road_graph.edges FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_geometry_insert_or_update();


-- editing_sessions trg_aa_before_geometry_insert_or_update
CREATE TRIGGER trg_aa_before_geometry_insert_or_update BEFORE INSERT OR UPDATE OF geom ON road_graph.editing_sessions FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_geometry_insert_or_update();


-- markers trg_aa_before_geometry_insert_or_update
CREATE TRIGGER trg_aa_before_geometry_insert_or_update BEFORE INSERT OR UPDATE OF geom ON road_graph.markers FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_geometry_insert_or_update();


-- nodes trg_aa_before_geometry_insert_or_update
CREATE TRIGGER trg_aa_before_geometry_insert_or_update BEFORE INSERT OR UPDATE OF geom ON road_graph.nodes FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_geometry_insert_or_update();


-- edges trg_after_edge_delete
CREATE TRIGGER trg_after_edge_delete AFTER DELETE ON road_graph.edges FOR EACH ROW EXECUTE PROCEDURE road_graph.after_edge_delete();


-- edges trg_after_edge_insert_or_update
CREATE TRIGGER trg_after_edge_insert_or_update AFTER INSERT OR UPDATE ON road_graph.edges FOR EACH ROW EXECUTE PROCEDURE road_graph.after_edge_insert_or_update();


-- markers trg_after_marker_insert_or_update_or_delete
CREATE TRIGGER trg_after_marker_insert_or_update_or_delete AFTER INSERT OR DELETE OR UPDATE ON road_graph.markers FOR EACH ROW EXECUTE PROCEDURE road_graph.after_marker_insert_or_update_or_delete();


-- nodes trg_after_node_insert_or_update
CREATE TRIGGER trg_after_node_insert_or_update AFTER INSERT OR UPDATE ON road_graph.nodes FOR EACH ROW EXECUTE PROCEDURE road_graph.after_node_insert_or_update();


-- edges trg_before_edge_insert_or_update
CREATE TRIGGER trg_before_edge_insert_or_update BEFORE INSERT OR UPDATE ON road_graph.edges FOR EACH ROW EXECUTE PROCEDURE road_graph.before_edge_insert_or_update();


-- editing_sessions trg_before_editing_sessions_update
CREATE TRIGGER trg_before_editing_sessions_update BEFORE UPDATE OF geom ON road_graph.editing_sessions FOR EACH ROW EXECUTE PROCEDURE road_graph.before_editing_sessions_update();


--
-- PostgreSQL database dump complete
--



